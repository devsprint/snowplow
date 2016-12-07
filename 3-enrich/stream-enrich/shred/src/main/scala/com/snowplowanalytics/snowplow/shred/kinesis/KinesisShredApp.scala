package com.snowplowanalytics.snowplow.shred.kinesis

import java.io.File
import java.net.URI

import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient
import com.amazonaws.services.dynamodbv2.document.DynamoDB
import com.amazonaws.services.dynamodbv2.model.{AttributeValue, ScanRequest}
import com.amazonaws.services.s3.AmazonS3Client
import com.amazonaws.services.s3.model.GetObjectRequest
import com.snowplowanalytics.iglu.client.Resolver
import com.snowplowanalytics.snowplow.enrich.common.enrichments.EnrichmentRegistry
import com.snowplowanalytics.snowplow.enrich.common.utils.JsonUtils
import com.snowplowanalytics.snowplow.enrich.kinesis.sources.{KafkaSource, KinesisEnrichedSource, StdinSource}
import com.snowplowanalytics.snowplow.enrich.kinesis._
import com.typesafe.config.{Config, ConfigFactory}
import org.clapper.argot.ArgotParser
import org.json4s.JsonDSL._
import org.json4s.jackson.JsonMethods.{compact, fromJsonNode, parse}
import org.slf4j.LoggerFactory

import scala.annotation.tailrec
import scala.collection.JavaConverters._
import scala.sys.process._
import scalaz.Scalaz._

/**
  * Implement the shredding process from an enriched kinesis stream.
  */
object KinesisShredApp extends App {

  lazy val log = LoggerFactory.getLogger(getClass())

  import log.{error, info}

  val FilepathRegex = "^file:(.+)$".r
  val DynamoDBRegex = "^dynamodb:([^/]*)/([^/]*)/([^/]*)$".r

  val parser = new ArgotParser(
    programName = generated.Settings.name,
    compactUsage = true,
    preUsage = Some("%s: Version %s. Copyright (c) 2013, %s.".format(
      generated.Settings.name,
      generated.Settings.version,
      generated.Settings.organization)
    )
  )

  // Mandatory config argument
  val config = parser.option[Config](
    List("config"), "filename",
    """
      |Configuration file.""".stripMargin) {
    (c, opt) =>
      val file = new File(c)
      if (file.exists) {
        ConfigFactory.parseFile(file).resolve()
      } else {
        parser.usage("Configuration file \"%s\" does not exist".format(c))
        ConfigFactory.empty()
      }
  }

  // Mandatory resolver argument
  val resolverOption = parser.option[String](
    List("resolver"), "'file:[filename]' or 'dynamodb:[region/table/key]'",
    """
      |Iglu resolver file.""".stripMargin) {
    (c, opt) => c
  }

  // Optional directory of enrichment configuration JSONs
  val enrichmentsOption = parser.option[String](
    List("enrichments"), "'file:[filename]' or 'dynamodb:[region/table/partialKey]'",
    """
      |Directory of enrichment configuration JSONs.""".stripMargin) {
    (c, opt) => c
  }

  parser.parse(args)

  val parsedConfig = config.value.getOrElse(parser.usage("--config argument must be provided"))
  private val shred = parsedConfig.resolve.getConfig("shred")
  val kinesisConfig = new KinesisConfig(shred)

  val tracker = if (parsedConfig.hasPath("enrich.monitoring.snowplow")) {
    SnowplowTracking.initializeTracker(parsedConfig.getConfig("enrich.monitoring.snowplow")).some
  } else {
    None
  }

  val nonOptionalResolver = resolverOption.value.getOrElse(parser.usage("--resolver argument must be provided"))
  val parsedResolver = extractResolver(nonOptionalResolver)

  val enrichmentConfig = extractEnrichmentConfig(enrichmentsOption.value)

  implicit val igluResolver: Resolver = (for {
    json <- JsonUtils.extractJson("", parsedResolver)
    resolver <- Resolver.parse(json).leftMap(_.toString)
  } yield resolver) fold(
    e => throw new RuntimeException(e),
    s => s
  )

  val registry: EnrichmentRegistry = (for {
    registryConfig <- JsonUtils.extractJson("", enrichmentConfig)
    reg <- EnrichmentRegistry.parse(fromJsonNode(registryConfig), false).leftMap(_.toString)
  } yield reg) fold(
    e => throw new RuntimeException(e),
    s => s
  )

  val filesToCache = registry.getIpLookupsEnrichment match {
    case Some(ipLookups) => ipLookups.dbsToCache
    case None => Nil
  }

  for (uriFilePair <- filesToCache) {
    val targetFile = new File(uriFilePair._2)

    // Ensure uri does not have doubled slashes
    val cleanUri = new java.net.URI(uriFilePair._1.toString.replaceAll("(?<!(http:|https:|s3:))//", "/"))

    // Download the database file if it doesn't already exist or is empty
    // See http://stackoverflow.com/questions/10281370/see-if-file-is-empty
    if (targetFile.length == 0L) {

      // Check URI Protocol and download file
      val downloadResult: Int = cleanUri.getScheme match {
        case "http" | "https" => (cleanUri.toURL #> targetFile).! // using sys.process
        case "s3" => downloadFromS3(cleanUri, targetFile)
        case s => throw new RuntimeException(s"Schema ${s} for file ${cleanUri} not supported")
      }

      if (downloadResult != 0) {
        throw new RuntimeException(s"Attempt to download ${cleanUri} to $targetFile failed")
      }
    }
  }

  val shredSource = kinesisConfig.source match {
    case Source.Kafka => new KafkaSource(kinesisConfig, igluResolver, registry, tracker)
    case Source.Kinesis => new KinesisEnrichedSource(kinesisConfig, igluResolver, registry, tracker)
    case Source.Stdin => new StdinSource(kinesisConfig, igluResolver, registry, tracker)
  }

  tracker match {
    case Some(t) => SnowplowTracking.initializeSnowplowTracking(t)
    case None => None
  }


  shredSource.run

  /**
    * Return a JSON string based on the resolver argument
    *
    * @param resolverArgument
    * @return JSON from a local file or stored in DynamoDB
    */
  def extractResolver(resolverArgument: String): String = resolverArgument match {
    case FilepathRegex(filepath) => {
      val file = new File(filepath)
      if (file.exists) {
        scala.io.Source.fromFile(file).mkString
      } else {
        parser.usage("Iglu resolver configuration file \"%s\" does not exist".format(filepath))
      }
    }
    case DynamoDBRegex(region, table, key) => lookupDynamoDBConfig(region, table, key)
    case _ => parser.usage(s"Resolver argument [$resolverArgument] must begin with 'file:' or 'dynamodb:'")
  }

  /**
    * Fetch configuration from DynamoDB
    * Assumes the primary key is "id" and the configuration's key is "json"
    *
    * @param region DynamoDB region, e.g. "eu-west-1"
    * @param table
    * @param key    The value of the primary key for the configuration
    * @return The JSON stored in DynamoDB
    */
  def lookupDynamoDBConfig(region: String, table: String, key: String): String = {
    val dynamoDBClient = new AmazonDynamoDBClient(kinesisConfig.credentialsProvider)
    dynamoDBClient.setEndpoint(s"https://dynamodb.${region}.amazonaws.com")
    val dynamoDB = new DynamoDB(dynamoDBClient)
    val item = dynamoDB.getTable(table).getItem("id", key)
    item.getString("json")
  }

  /**
    * Return an enrichment configuration JSON based on the enrichments argument
    *
    * @param enrichmentArgument
    * @return JSON containing configuration for all enrichments
    */
  def extractEnrichmentConfig(enrichmentArgument: Option[String]): String = {
    val jsons: Iterable[String] = enrichmentArgument match {
      case None => Nil
      case Some(FilepathRegex(dir)) => new java.io.File(dir).listFiles.filter(_.getName.endsWith(".json"))
        .map(scala.io.Source.fromFile(_).mkString)
      case Some(DynamoDBRegex(region, table, partialKey)) => {
        (lookupDynamoDBEnrichments(region, table, partialKey), tracker) match {
          case (Nil, Some(t)) => {
            SnowplowTracking.trackApplicationWarning(t, s"No enrichments found with partial key ${partialKey}")
            Nil
          }
          case (Nil, None) => {
            info(s"No enrichments found with partial key ${partialKey}")
            Nil
          }
          case (jsons, _) => jsons
        }
      }
      case Some(other) => parser.usage(s"Enrichments argument [$other] must begin with 'file:' or 'dynamodb:'")
    }
    val combinedJson = ("schema" -> "iglu:com.snowplowanalytics.snowplow/enrichments/jsonschema/1-0-0") ~
      ("data" -> jsons.toList.map(parse(_)))
    compact(combinedJson)
  }

  /**
    * Get a list of enrichment JSONs from DynamoDB
    *
    * @param region     DynamoDB region, e.g. "eu-west-1"
    * @param table
    * @param partialKey Primary key prefix, e.g. "enrichments-"
    * @return List of JSONs
    */
  def lookupDynamoDBEnrichments(region: String, table: String, partialKey: String): List[String] = {
    val dynamoDBClient = new AmazonDynamoDBClient(kinesisConfig.credentialsProvider)
    dynamoDBClient.setEndpoint(s"https://dynamodb.${region}.amazonaws.com")

    // Each scan can only return up to 1MB
    // See http://techtraits.com/cloud/nosql/2012/06/27/Amazon-DynamoDB--Understanding-Query-and-Scan-operations/
    @tailrec
    def partialScan(sofar: List[Map[String, String]] = Nil, lastEvaluatedKey: java.util.Map[String, AttributeValue] = null): List[Map[String, String]] = {
      val scanRequest = new ScanRequest().withTableName(table)
      scanRequest.setExclusiveStartKey(lastEvaluatedKey)
      val lastResult = dynamoDBClient.scan(scanRequest)
      val combinedResults = sofar ++ lastResult.getItems.asScala.map(_.asScala.toMap.mapValues(_.getS))
      lastResult.getLastEvaluatedKey match {
        case null => combinedResults
        case startKey => partialScan(combinedResults, startKey)
      }
    }

    val allItems = partialScan(Nil)
    allItems filter { item =>
      item.get("id") match {
        case Some(value) if value.startsWith(partialKey) => true
        case _ => false
      }
    } flatMap (_.get("json"))
  }

  /**
    * Downloads an object from S3 and returns whether
    * or not it was successful.
    *
    * @param uri        The URI to reconstruct into a signed
    *                   S3 URL
    * @param outputFile The file object to write to
    * @return the download result
    */
  def downloadFromS3(uri: URI, outputFile: File): Int = {
    val s3Client = new AmazonS3Client(kinesisConfig.credentialsProvider)
    val bucket = uri.getHost
    val key = uri.getPath match {
      // Need to remove leading '/'
      case s if s.charAt(0) == '/' => s.substring(1)
      case s => s
    }

    try {
      s3Client.getObject(new GetObjectRequest(bucket, key), outputFile)
      0
    } catch {
      case e: Exception => {
        error(s"Error downloading ${uri}: ${e.toString}")
        1
      }
    }
  }

}
