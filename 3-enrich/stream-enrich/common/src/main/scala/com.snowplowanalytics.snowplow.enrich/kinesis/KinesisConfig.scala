package com.snowplowanalytics.snowplow.enrich.kinesis

import com.typesafe.config.Config

/**
  * Configuration helper.
  */
// Rigidly load the configuration file here to error when
// the enrichment process starts rather than later.
class KinesisConfig(config: Config) {

  val source = config.getString("source") match {
    case "kafka" => Source.Kafka
    case "kinesis" => Source.Kinesis
    case "stdin" => Source.Stdin
    case "test" => Source.Test
    case _ => throw new RuntimeException("enrich.source unknown.")
  }

  val sink = config.getString("sink") match {
    case "kafka" => Sink.Kafka
    case "kinesis" => Sink.Kinesis
    case "stdouterr" => Sink.Stdouterr
    case "test" => Sink.Test
    case _ => throw new RuntimeException("enrich.sink unknown.")
  }

  private val aws = config.getConfig("aws")
  val accessKey = aws.getString("access-key")
  val secretKey = aws.getString("secret-key")

  private val kafka = config.getConfig("kafka")
  val kafkaBrokers = kafka.getString("brokers")

  private val streams = config.getConfig("streams")

  private val inStreams = streams.getConfig("in")
  val rawInStream = inStreams.getString("raw")

  private val outStreams = streams.getConfig("out")
  val enrichedOutStream = outStreams.getString("enriched")
  val badOutStream = outStreams.getString("bad")

  val appName = streams.getString("app-name")

  val initialPosition = streams.getString("initial-position")

  val streamRegion = streams.getString("region")
  val streamEndpoint = s"https://kinesis.${streamRegion}.amazonaws.com"

  val maxRecords = if (inStreams.hasPath("maxRecords")) {
    inStreams.getInt("maxRecords")
  } else {
    10000
  }

  val buffer = inStreams.getConfig("buffer")
  val byteLimit = buffer.getInt("byte-limit")
  val recordLimit = buffer.getInt("record-limit")
  val timeLimit = buffer.getInt("time-limit")

  val credentialsProvider = CredentialsLookup.getCredentialsProvider(accessKey, secretKey)

  val backoffPolicy = outStreams.getConfig("backoffPolicy")
  val minBackoff = backoffPolicy.getLong("minBackoff")
  val maxBackoff = backoffPolicy.getLong("maxBackoff")

  val useIpAddressAsPartitionKey = outStreams.hasPath("useIpAddressAsPartitionKey") && outStreams.getBoolean("useIpAddressAsPartitionKey")
}
