package com.snowplowanalytics.snowplow.shred.stream.sources

import java.net.InetAddress
import java.nio.charset.StandardCharsets
import java.util
import java.util.{List, UUID}

import com.amazonaws.auth.AWSCredentialsProvider
import com.amazonaws.client.builder.AwsClientBuilder.EndpointConfiguration
import com.amazonaws.services.kinesis.AmazonKinesisClientBuilder
import com.amazonaws.services.kinesis.clientlibrary.exceptions.{InvalidStateException, ShutdownException, ThrottlingException}
import com.amazonaws.services.kinesis.clientlibrary.interfaces.{IRecordProcessor, IRecordProcessorCheckpointer, IRecordProcessorFactory}
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.{InitialPositionInStream, KinesisClientLibConfiguration, ShutdownReason, Worker}
import com.amazonaws.services.kinesis.model.Record
import com.snowplowanalytics.iglu.client.{ProcessingMessageNel, Resolver}
import com.snowplowanalytics.snowplow.enrich.common.{JsonSchemaPairs, ValidatedNelMessage}
import com.snowplowanalytics.snowplow.enrich.common.enrichments.EnrichmentRegistry
import com.snowplowanalytics.snowplow.enrich.stream.model._
import com.snowplowanalytics.snowplow.enrich.stream.sinks.{KinesisSink, Sink}
import com.snowplowanalytics.snowplow.enrich.stream.sources.Source
import com.snowplowanalytics.snowplow.scalatracker.Tracker
import com.snowplowanalytics.snowplow.shred.stream.KinesisShred
import com.snowplowanalytics.snowplow.shred.stream.sinks.{FirehoseDeliveryStreams, FirehoseSink}
import com.snowplowanalytics.snowplow.shred.stream.sources.ShredJob.EventComponents
import scalaz.Validation

import scala.util.control.Breaks._
import scala.collection.JavaConversions._
import scala.util.control.NonFatal
import scalaz._
import Scalaz._
import com.amazonaws.services.kinesisfirehose.AmazonKinesisFirehoseClientBuilder
import com.snowplowanalytics.snowplow.shred.stream.sinks.Contexts._

/** KinesisSource companion object with factory method */
object KinesisEnrichedSource {
  def createAndInitialize(
                           config: StreamsConfig,
                           igluResolver: Resolver,
                           enrichmentRegistry: EnrichmentRegistry,
                           tracker: Option[Tracker]
                         ): Validation[String, KinesisEnrichedSource] = for {
    kinesisConfig <- config.sourceSink match {
      case c: Kinesis => c.success
      case _ => "Configured source/sink is not Kinesis".failure
    }
    _ <- (FirehoseSink.validate(kinesisConfig, config.out.enriched).validation.leftMap(_.wrapNel) |@|
      KinesisSink.validate(kinesisConfig, config.out.bad).validation.leftMap(_.wrapNel)) {
      (_, _) => ()
    }.leftMap(_.toList.mkString("\n"))
    provider <- KinesisShred.getProvider(kinesisConfig.aws).validation
  } yield new KinesisEnrichedSource(igluResolver, enrichmentRegistry, tracker, config, kinesisConfig, provider)
}

/**
  * Read the data from enriched stream source.
  * TODO: the implementation is very similar with the Kinesis Source - it may be refactored.
  */
class KinesisEnrichedSource(igluResolver: Resolver,
                            enrichmentRegistry: EnrichmentRegistry,
                            tracker: Option[Tracker],
                            config: StreamsConfig,
                            kinesisConfig: Kinesis,
                            provider: AWSCredentialsProvider)
  extends Source(igluResolver, enrichmentRegistry, tracker, config.out.partitionKey) {


  import log.{error, info}

  override val MaxRecordSize = Some(1000000L)

  private val client = {
    val endpointConfiguration =
      new EndpointConfiguration(kinesisConfig.streamEndpoint, kinesisConfig.region)
    AmazonKinesisFirehoseClientBuilder
      .standard()
      .withCredentials(provider)
      .withRegion(kinesisConfig.region)
      .build()
  }

  private val KinesisClient = {
    val endpointConfiguration =
      new EndpointConfiguration(kinesisConfig.streamEndpoint, kinesisConfig.region)
    AmazonKinesisClientBuilder
      .standard()
      .withCredentials(provider)
      .withEndpointConfiguration(endpointConfiguration)
      .build()
  }


  override val threadLocalGoodSink: ThreadLocal[Sink] = new ThreadLocal[Sink] {
    override def initialValue: Sink =
      new FirehoseSink(client, kinesisConfig.backoffPolicy, config.buffer, FirehoseDeliveryStreams.Firehose, tracker)
  }

  val uaFirehoseSink = new ThreadLocal[Sink] {
    override def initialValue: Sink =
      new FirehoseSink(client, kinesisConfig.backoffPolicy, config.buffer, FirehoseDeliveryStreams.UAFirehose, tracker)
  }
  val performanceFirehoseSink = new ThreadLocal[Sink] {
    override def initialValue: Sink =
      new FirehoseSink(client, kinesisConfig.backoffPolicy, config.buffer, FirehoseDeliveryStreams.PerformanceFirehose, tracker)
  }
  val googleAnalyticsCookiesSink = new ThreadLocal[Sink] {
    override def initialValue: Sink =
      new FirehoseSink(client, kinesisConfig.backoffPolicy, config.buffer, FirehoseDeliveryStreams.GoogleAnalyticsCookiesFirehose, tracker)
  }
  val googleAnalyticsSocialSink = new ThreadLocal[Sink] {
    override def initialValue: Sink =
      new FirehoseSink(client, kinesisConfig.backoffPolicy, config.buffer, FirehoseDeliveryStreams.GoogleAnalyticsSocialFirehose, tracker)
  }
  val googleAnalyticsProductImpressionSink = new ThreadLocal[Sink] {
    override def initialValue: Sink =
      new FirehoseSink(client, kinesisConfig.backoffPolicy, config.buffer, FirehoseDeliveryStreams.GoogleAnalyticsProductImpressionFirehose, tracker)
  }
  val googleAnalyticsTransactionSink = new ThreadLocal[Sink] {
    override def initialValue: Sink =
      new FirehoseSink(client, kinesisConfig.backoffPolicy, config.buffer, FirehoseDeliveryStreams.GoogleAnalyticsTransactionFirehose, tracker)
  }
  val googleAnalyticsEecomActionSink = new ThreadLocal[Sink] {
    override def initialValue: Sink =
      new FirehoseSink(client, kinesisConfig.backoffPolicy, config.buffer, FirehoseDeliveryStreams.GoogleAnalyticsEecomActionFirehose, tracker)
  }
  val googleAnalyticsEecomActionFieldSink = new ThreadLocal[Sink] {
    override def initialValue: Sink =
      new FirehoseSink(client, kinesisConfig.backoffPolicy, config.buffer, FirehoseDeliveryStreams.GoogleAnalyticsEecomActionFieldFirehose, tracker)
  }
  val googleAnalyticsEecomImpressionFieldSink = new ThreadLocal[Sink] {
    override def initialValue: Sink =
      new FirehoseSink(client, kinesisConfig.backoffPolicy, config.buffer, FirehoseDeliveryStreams.GoogleAnalyticsEecomImpressionFieldFirehose, tracker)
  }
  val googleAnalyticsEecomProductFieldSink = new ThreadLocal[Sink] {
    override def initialValue: Sink =
      new FirehoseSink(client, kinesisConfig.backoffPolicy, config.buffer, FirehoseDeliveryStreams.GoogleAnalyticsEecomProductFieldFirehose, tracker)
  }
  val googleAnalyticsEecomPromoFieldSink = new ThreadLocal[Sink] {
    override def initialValue: Sink =
      new FirehoseSink(client, kinesisConfig.backoffPolicy, config.buffer, FirehoseDeliveryStreams.GoogleAnalyticsEecomPromoFieldFirehose, tracker)
  }


  override val threadLocalBadSink: ThreadLocal[Sink] = new ThreadLocal[Sink] {
    override def initialValue: Sink =
      new KinesisSink(KinesisClient, kinesisConfig.backoffPolicy, config.buffer, config.out.bad, tracker)
  }


  /**
    * Never-ending processing loop over source stream.
    */
  override def run: Unit = {
    val workerId = InetAddress.getLocalHost().getCanonicalHostName() + ":" + UUID.randomUUID()
    log.info("Using workerId: " + workerId)

    val kinesisClientLibConfiguration = {
      val kclc = new KinesisClientLibConfiguration(
        config.appName,
        config.in.raw,
        provider,
        workerId
      ).withKinesisEndpoint(kinesisConfig.streamEndpoint)
        .withMaxRecords(kinesisConfig.maxRecords)
        .withRegionName(kinesisConfig.region)
        // If the record list is empty, we still check whether it is time to flush the buffer
        .withCallProcessRecordsEvenForEmptyRecordList(true)

      val position = InitialPositionInStream.valueOf(kinesisConfig.initialPosition)
      kinesisConfig.timestamp.right.toOption
        .filter(_ => position == InitialPositionInStream.AT_TIMESTAMP)
        .map(kclc.withTimestampAtInitialPositionInStream(_))
        .getOrElse(kclc.withInitialPositionInStream(position))
    }

    log.info(s"Running: ${config.appName}.")
    log.info(s"Processing raw input stream: ${config.in.raw}")

    val rawEventProcessorFactory = new EnrichedEventProcessorFactory(
      config,
      threadLocalGoodSink.get // TODO: yech, yech
    )
    val worker = new Worker(
      rawEventProcessorFactory,
      kinesisClientLibConfiguration
    )

    worker.run()
  }

  def shredAndStoreEvents(events: List[String]): Boolean = {

    val common: Seq[ValidatedNelMessage[(String, String, JsonSchemaPairs, String)]] = events.map { line: String =>
      ShredJob.loadAndShred(line)
    }

    val bad: Seq[ProcessingMessageNel] = common.flatMap { o: ValidatedNelMessage[EventComponents] =>
      ShredJob.projectBads(o)
    }

    info(s"Derived Data errors: $bad")

    val errors = threadLocalBadSink.get().storeEnrichedEvents(bad.map(t => (t.toString(), "1")).toList)

    val good: Seq[(String, String, JsonSchemaPairs, String)] = common.flatMap { o: ValidatedNelMessage[EventComponents] =>
      ShredJob.projectGoods(o)
    }

    val goodPairs = good.flatMap(t => t._3).map(t => (t._2.toString, t._1.name)).toList
    //info(s"Derived Data jsons: $goodPairs")
    val uaContextErrors: Boolean = uaFirehoseSink.get().storeEnrichedEvents(goodPairs.filter(_._2 == UA_PARSER_CONTEXT))

    val performanceContextErrors = performanceFirehoseSink.get().storeEnrichedEvents(goodPairs.filter(_._2 == PERFORMANCE_TIMING_CONTEXT))

    val googleAnalyticsCookiesErrors = googleAnalyticsCookiesSink.get().storeEnrichedEvents(goodPairs.filter(_._2 == GOOGLE_ANALYTICS_COOKIES_CONTEXT))

    val googleAnalyticsSocialErrors = googleAnalyticsSocialSink.get().storeEnrichedEvents(goodPairs.filter(_._2 == GOOGLE_ANALYTICS_SOCIAL_CONTEXT))

    val googleAnalyticsProductImpressionErrors = googleAnalyticsProductImpressionSink.get().storeEnrichedEvents(goodPairs.filter(_._2 == GOOGLE_ANALYTICS_PRODUCT_IMPRESSION_CONTEXT))

    val googleAnalyticsTransactionErrors = googleAnalyticsTransactionSink.get().storeEnrichedEvents(goodPairs.filter(_._2 == GOOGLE_ANALYTICS_TRANZACTION_CONTEXT))

    val googleAnalyticsEecomActionErrors = googleAnalyticsEecomActionSink.get().storeEnrichedEvents(goodPairs.filter(_._2 == GOOGLE_ANALYTICS_EECOM_ACTION_CONTEXT))

    val googleAnalyticsEecomActionFieldErrors = googleAnalyticsEecomActionFieldSink.get().storeEnrichedEvents(goodPairs.filter(_._2 == GOOGLE_ANALYTICS_EECOM_ACTION_FIELD_CONTEXT))

    val googleAnalyticsEecomImpressionFieldErrors = googleAnalyticsEecomImpressionFieldSink.get().storeEnrichedEvents(goodPairs.filter(_._2 == GOOGLE_ANALYTICS_EECOM_IMPRESSION_FIELD_CONTEXT))

    val googleAnalyticsEecomProductFieldErrors = googleAnalyticsEecomProductFieldSink.get().storeEnrichedEvents(goodPairs.filter(_._2 == GOOGLE_ANALYTICS_EECOM_PRODUCT_FIELD_CONTEXT))

    val googleAnalyticsEecomPromoFieldErrors = googleAnalyticsEecomPromoFieldSink.get().storeEnrichedEvents(goodPairs.filter(_._2 == GOOGLE_ANALYTICS_EECOM_PROMO_FIELD_CONTEXT))


    if (
      uaContextErrors ||
        performanceContextErrors ||
        googleAnalyticsCookiesErrors ||
        googleAnalyticsSocialErrors ||
        googleAnalyticsProductImpressionErrors ||
        googleAnalyticsTransactionErrors ||
        googleAnalyticsEecomActionErrors ||
        googleAnalyticsEecomActionFieldErrors ||
        googleAnalyticsEecomImpressionFieldErrors ||
        googleAnalyticsEecomProductFieldErrors ||
        googleAnalyticsEecomPromoFieldErrors

    ) {
      uaFirehoseSink.get().flush()
      performanceFirehoseSink.get().flush()
      googleAnalyticsCookiesSink.get().flush()
      googleAnalyticsSocialSink.get().flush()
      googleAnalyticsProductImpressionSink.get().flush()
      googleAnalyticsTransactionSink.get().flush()
      googleAnalyticsEecomActionSink.get().flush()
      googleAnalyticsEecomActionFieldSink.get().flush()
      googleAnalyticsEecomImpressionFieldSink.get().flush()
      googleAnalyticsEecomProductFieldSink.get().flush()
      googleAnalyticsEecomPromoFieldSink.get().flush()
      true
    } else {
      false
    }
  }

  def storeAlteredAtomicEvents(originalEvents: List[String]): Boolean = {

    val altered = originalEvents.map(ShredJob.alterEnrichedEvent)
    val alteredWithPartitionKey = altered.map {
      event =>
        (event,
          UUID.randomUUID.toString
        )
    }
    val (tooBigSuccesses, smallEnoughSuccesses) = alteredWithPartitionKey partition { s => isTooLarge(s._1) }

    val sizeBasedFailures = for {
      (value, key) <- tooBigSuccesses
      m <- MaxRecordSize
    } yield Source.oversizedSuccessToFailure(value, m) -> key

    val successesTriggeredFlush = threadLocalGoodSink.get().storeEnrichedEvents(smallEnoughSuccesses.toList)
    if (sizeBasedFailures.nonEmpty) {
      error(s"Too large records: $sizeBasedFailures")
    }

    if (successesTriggeredFlush) {
      threadLocalGoodSink.get().flush()
      true
    } else {
      false
    }
  }


  class EnrichedEventProcessorFactory(config: StreamsConfig, sink: Sink) extends IRecordProcessorFactory {
    override def createProcessor(): IRecordProcessor = {
      new EnrichedEventProcessor(config, sink)
    }
  }

  class EnrichedEventProcessor(config: StreamsConfig, sink: Sink) extends IRecordProcessor {

    private var kinesisShardId: String = _

    // Backoff and retry settings.
    private val BACKOFF_TIME_IN_MILLIS = 3000L
    private val NUM_RETRIES = 10
    private val CHECKPOINT_INTERVAL_MILLIS = 1000L


    override def initialize(shardId: String): Unit = {
      info("Initializing record processor for shard: " + shardId)
      this.kinesisShardId = shardId
    }

    override def processRecords(records: util.List[Record], checkpointer: IRecordProcessorCheckpointer): Unit = {
      if (!records.isEmpty) {
        info(s"Processing ${records.size} records from $kinesisShardId")
      }
      val shouldCheckpoint = processRecordsWithRetries(records)

      if (shouldCheckpoint) {
        checkpoint(checkpointer)
      }
    }

    private def processRecordsWithRetries(records: List[Record]): Boolean = {
      try {
        val events = records.map(_.getData.array).map(t => new String(t, StandardCharsets.UTF_8)).toList
        storeAlteredAtomicEvents(events)
        shredAndStoreEvents(events)
        true
      } catch {
        case NonFatal(e) =>
          // TODO: send an event when something goes wrong here
          error(s"Caught throwable while processing records $records", e)
          false
      }
    }

    override def shutdown(checkpointer: IRecordProcessorCheckpointer,
                          reason: ShutdownReason) = {
      log.info(s"Shutting down record processor for shard: $kinesisShardId")
      if (reason == ShutdownReason.TERMINATE) {
        checkpoint(checkpointer)
      }
    }


    private def checkpoint(checkpointer: IRecordProcessorCheckpointer) = {
      info(s"Checkpointing shard $kinesisShardId")
      breakable {
        for (i <- 0 to NUM_RETRIES - 1) {
          try {
            checkpointer.checkpoint()
            break
          } catch {
            case se: ShutdownException =>
              error("Caught shutdown exception, skipping checkpoint.", se)
              break
            case e: ThrottlingException =>
              if (i >= (NUM_RETRIES - 1)) {
                error(s"Checkpoint failed after ${i + 1} attempts.", e)
              } else {
                info(s"Transient issue when checkpointing - attempt ${i + 1} of "
                  + NUM_RETRIES, e)
              }
            case e: InvalidStateException =>
              error("Cannot save checkpoint to the DynamoDB table used by " +
                "the Amazon Kinesis Client Library.", e)
              break
          }
          Thread.sleep(BACKOFF_TIME_IN_MILLIS)
        }
      }
    }
  }


}
