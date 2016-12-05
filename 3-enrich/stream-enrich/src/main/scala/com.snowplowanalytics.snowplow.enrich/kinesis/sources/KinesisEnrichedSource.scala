package com.snowplowanalytics.snowplow.enrich.kinesis.sources

import java.net.InetAddress
import java.util
import java.util.{List, UUID}

import com.amazonaws.services.kinesis.clientlibrary.exceptions.{InvalidStateException, ShutdownException, ThrottlingException}
import com.amazonaws.services.kinesis.clientlibrary.interfaces.{IRecordProcessor, IRecordProcessorCheckpointer, IRecordProcessorFactory}
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.{InitialPositionInStream, KinesisClientLibConfiguration, Worker}
import com.amazonaws.services.kinesis.clientlibrary.types.ShutdownReason
import com.amazonaws.services.kinesis.model.Record
import com.snowplowanalytics.iglu.client.Resolver
import com.snowplowanalytics.snowplow.enrich.common.enrichments.EnrichmentRegistry
import com.snowplowanalytics.snowplow.enrich.kinesis.KinesisEnrichConfig
import com.snowplowanalytics.snowplow.enrich.kinesis.sinks.ISink
import com.snowplowanalytics.snowplow.scalatracker.Tracker
import org.slf4j.LoggerFactory

import scala.util.control.Breaks.{break, breakable}
import scala.util.control.NonFatal

import scala.collection.JavaConversions._

/**
  * Read the data from enriched stream source.
  * TODO: the implementation is very similar with the Kinesis Source - it may be refactored.
  */
class KinesisEnrichedSource(config: KinesisEnrichConfig, igluResolver: Resolver, enrichmentRegistry: EnrichmentRegistry, tracker: Option[Tracker])
  extends AbstractSource(config, igluResolver, enrichmentRegistry, tracker) {

  lazy val log = LoggerFactory.getLogger(getClass())
  import log.{error, debug, info, trace}



  /**
    * Never-ending processing loop over source stream.
    */
  override def run: Unit = {
    val workerId = InetAddress.getLocalHost().getCanonicalHostName() +
      ":" + UUID.randomUUID()
    info("Using workerId: " + workerId)

    val kinesisClientLibConfiguration = new KinesisClientLibConfiguration(
      config.appName,
      config.enrichedOutStream,
      kinesisProvider,
      workerId
    ).withInitialPositionInStream(
      InitialPositionInStream.valueOf(config.initialPosition)
    ).withKinesisEndpoint(config.streamEndpoint)
      .withMaxRecords(config.maxRecords)
      .withRegionName(config.streamRegion)
      // If the record list is empty, we still check whether it is time to flush the buffer
      .withCallProcessRecordsEvenForEmptyRecordList(true)

    info(s"Running: ${config.appName}.")
    info(s"Processing enriched input stream: ${config.enrichedOutStream}")

    val enrichedEventProcessorFactory = new EnrichedEventProcessorFactory(
      config,
      shredSink.get.get // TODO: yech, yech
    )
    val worker = new Worker(
      enrichedEventProcessorFactory,
      kinesisClientLibConfiguration
    )

    worker.run()
  }

  class EnrichedEventProcessorFactory(config: KinesisEnrichConfig, sink: ISink) extends IRecordProcessorFactory {
    override def createProcessor(): IRecordProcessor = {
      new EnrichedEventProcessor(config, sink)
    }
  }

  class EnrichedEventProcessor(config: KinesisEnrichConfig, sink: ISink) extends  IRecordProcessor {

    private var kinesisShardId: String = _

    // Backoff and retry settings.
    private val BACKOFF_TIME_IN_MILLIS = 3000L
    private val NUM_RETRIES = 10
    private val CHECKPOINT_INTERVAL_MILLIS = 1000L


    override def shutdown(checkpointer: IRecordProcessorCheckpointer, reason: ShutdownReason): Unit = {
      info(s"Shutting down record processor for shard: $kinesisShardId")
      if (reason == ShutdownReason.TERMINATE) {
        checkpoint(checkpointer)
      }
    }

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
        // TODO: from an enriched event to shred.
        shredAndStoreEvents(records.map(_.getData.array).toList)
        true
      } catch {
        case NonFatal(e) =>
          // TODO: send an event when something goes wrong here
          error(s"Caught throwable while processing records $records", e)
          false
      }
    }



    private def checkpoint(checkpointer: IRecordProcessorCheckpointer) = {
      info(s"Checkpointing shard $kinesisShardId")
      breakable {
        for (i <- 0 to NUM_RETRIES-1) {
          try {
            checkpointer.checkpoint()
            break
          } catch {
            case se: ShutdownException =>
              error("Caught shutdown exception, skipping checkpoint.", se)
              break
            case e: ThrottlingException =>
              if (i >= (NUM_RETRIES - 1)) {
                error(s"Checkpoint failed after ${i+1} attempts.", e)
              } else {
                info(s"Transient issue when checkpointing - attempt ${i+1} of "
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
