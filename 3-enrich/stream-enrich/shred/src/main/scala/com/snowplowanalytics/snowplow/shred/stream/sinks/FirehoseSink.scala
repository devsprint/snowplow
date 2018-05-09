package com.snowplowanalytics.snowplow.shred.stream.sinks

import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets.UTF_8

import com.amazonaws.client.builder.AwsClientBuilder.EndpointConfiguration
import com.amazonaws.services.kinesisfirehose.model._
import com.amazonaws.services.kinesisfirehose.{AmazonKinesisFirehose, AmazonKinesisFirehoseClientBuilder}
import com.snowplowanalytics.snowplow.enrich.stream.{SnowplowTracking, utils}
import com.snowplowanalytics.snowplow.enrich.stream.model._
import com.snowplowanalytics.snowplow.enrich.stream.sinks.Sink
import com.snowplowanalytics.snowplow.scalatracker.Tracker
import com.snowplowanalytics.snowplow.shred.stream.KinesisShred
import scalaz.\/

import scala.util.control.NonFatal
import scala.collection.JavaConverters._
import scala.collection.immutable
import scala.concurrent.{Await, Future}
import scala.concurrent.ExecutionContext.Implicits.global
import scala.util.Try
import scala.concurrent.duration._

/** FirehoseSink companion object with factory method */
object FirehoseSink {
  def validate(kinesisConfig: Kinesis, firehoseName: String): \/[String, Unit] = for {
    provider <- KinesisShred.getProvider(kinesisConfig.aws)
    endpointConfiguration =
    new EndpointConfiguration(kinesisConfig.streamEndpoint, kinesisConfig.region)
    client = AmazonKinesisFirehoseClientBuilder
      .standard()
      .withCredentials(provider)
      .withRegion(kinesisConfig.region)
      .build()
    _ <- streamExists(client, firehoseName).leftMap(_.getMessage)
      .ensure(s"Kinesis firehose $firehoseName doesn't exist")(_ == true)
  } yield ()

  /**
    * Check whether a Kinesis stream exists
    *
    * @param firehoseName Name of the stream
    * @return Whether the stream exists
    */
  private def streamExists(client: AmazonKinesisFirehose, firehoseName: String): \/[Throwable, Boolean] = {
    val existsTry = Try {
      val deliveryStreamRequest = new DescribeDeliveryStreamRequest()
        .withDeliveryStreamName(firehoseName)
      val describeStreamResult = client.describeDeliveryStream(deliveryStreamRequest)
      val status = describeStreamResult.getDeliveryStreamDescription.getDeliveryStreamStatus
      status == "ACTIVE" || status == "UPDATING"
    }
    utils.toEither(existsTry)
  }
}

/**
  * Push the data to Firehose Delivery stream.
  */
class FirehoseSink(client: AmazonKinesisFirehose,
                   backoffPolicy: KinesisBackoffPolicyConfig,
                   buffer: BufferConfig,
                   firehoseName: String,
                   tracker: Option[Tracker]) extends Sink {

  /** Kinesis records must not exceed 1MB */
  private val MaxBytes = 1000000L

  private val maxBackoff = backoffPolicy.maxBackoff
  private val minBackoff = backoffPolicy.minBackoff
  private val randomGenerator = new java.util.Random()

  val ByteThreshold = buffer.byteLimit
  val RecordThreshold = buffer.recordLimit
  val TimeThreshold = buffer.timeLimit
  var nextRequestTime = 0L

  /**
    * Object to store events while waiting for the ByteThreshold, RecordThreshold, or TimeThreshold to be reached
    */
  object FirehoseEventStorage {
    // Each complete batch is the contents of a single PutRecords API call
    var completeBatches = List[List[(ByteBuffer, String)]]()
    // The batch currently under constructon
    var currentBatch = List[(ByteBuffer, String)]()
    // Length of the current batch
    var eventCount = 0
    // Size in bytes of the current batch
    var byteCount = 0

    /**
      * Finish work on the current batch and create a new one.
      */
    def sealBatch(): Unit = {
      completeBatches = currentBatch :: completeBatches
      eventCount = 0
      byteCount = 0
      currentBatch = Nil
    }

    /**
      * Add a new event to the current batch.
      * If this would take the current batch above ByteThreshold bytes,
      * first seal the current batch.
      * If this takes the current batch up to RecordThreshold records,
      * seal the current batch and make a new batch.
      *
      * @param event New event
      */
    def addEvent(event: (ByteBuffer, String)): Unit = {
      val newBytes = event._1.capacity

      if (newBytes >= MaxBytes) {
        val original = new String(event._1.array, UTF_8)
        log.error(s"Dropping record with size $newBytes bytes: [$original]")
      } else {

        if (byteCount + newBytes >= ByteThreshold) {
          sealBatch()
        }

        byteCount += newBytes

        eventCount += 1
        currentBatch = event :: currentBatch

        if (eventCount == RecordThreshold) {
          sealBatch()
        }
      }
    }

    /**
      * Reset everything.
      */
    def clear(): Unit = {
      completeBatches = Nil
      currentBatch = Nil
      eventCount = 0
      byteCount = 0
    }
  }

  /**
    * Side-effecting function to store the EnrichedEvent
    * to the given output stream.
    *
    * EnrichedEvent takes the form of a tab-delimited
    * String until such time as https://github.com/snowplow/snowplow/issues/211
    * is implemented.
    *
    * This method blocks until the request has finished.
    *
    * @param events List of events together with their partition keys
    * @return whether to send the stored events to Kinesis
    */
  override def storeEnrichedEvents(events: List[(String, String)]): Boolean = {
    val wrappedEvents = events.map(e => ByteBuffer.wrap(e._1.getBytes(UTF_8)) -> e._2)
    wrappedEvents.foreach(FirehoseEventStorage.addEvent(_))

    if (FirehoseEventStorage.currentBatch.nonEmpty && System.currentTimeMillis() > nextRequestTime) {
      nextRequestTime = System.currentTimeMillis() + TimeThreshold
      true
    } else {
      FirehoseEventStorage.completeBatches.nonEmpty
    }
  }

  /**
    * Blocking method to send all stored records to Kinesis
    * Splits the stored records into smaller batches (by byte size or record number) if necessary
    */
  override def flush(): Unit = {
    FirehoseEventStorage.sealBatch()
    // Send events in the order they were received
    FirehoseEventStorage.completeBatches.reverse.foreach(b => sendBatch(b.reverse))
    FirehoseEventStorage.clear()
  }

  /**
    * Send a single batch of events in one blocking PutRecords API call
    * Loop until all records have been sent successfully
    * Cannot be made tail recursive (http://stackoverflow.com/questions/8233089/why-wont-scala-optimize-tail-call-with-try-catch)
    *
    * @param batch Events to send
    */
  def sendBatch(batch: List[(ByteBuffer, String)]): Unit = {
    if (!batch.isEmpty) {
      log.info(s"Writing ${batch.size} records to Kinesis stream $firehoseName")
      var unsentRecords = batch
      var backoffTime = minBackoff
      var sentBatchSuccessfully = false
      var attemptNumber = 0
      while (!sentBatchSuccessfully) {
        attemptNumber += 1

        val putData = for {
          p <- multiPut(firehoseName, unsentRecords)
        } yield p

        try {
          val results = Await.result(putData, 10.seconds).getRequestResponses.asScala.toList
          val failurePairs = unsentRecords zip results filter {
            _._2.getErrorMessage != null
          }
          log.info(s"Successfully wrote ${unsentRecords.size - failurePairs.size} out of ${unsentRecords.size} records")
          if (failurePairs.nonEmpty) {
            val (failedRecords, failedResults) = failurePairs.unzip
            unsentRecords = failedRecords
            logErrorsSummary(getErrorsSummary(failedResults))
            backoffTime = getNextBackoff(backoffTime)
            log.error(s"Retrying all failed records in $backoffTime milliseconds...")

            val err = s"Failed to send ${failurePairs.size} events"
            val putSize: Long = unsentRecords.foldLeft(0L)((a, b) => a + b._1.capacity)

            tracker match {
              case Some(t) => SnowplowTracking.sendFailureEvent(t, "PUT Failure", err, firehoseName,
                "snowplow-stream-shred", attemptNumber.toLong, putSize)
              case _ => None
            }

            Thread.sleep(backoffTime)
          } else {
            sentBatchSuccessfully = true
          }
        } catch {
          case NonFatal(f) =>
            backoffTime = getNextBackoff(backoffTime)
            log.error(s"Writing failed.", f)
            log.error(s"  + Retrying in $backoffTime milliseconds...")

            val putSize: Long = unsentRecords.foldLeft(0L)((a, b) => a + b._1.capacity)

            tracker match {
              case Some(t) => SnowplowTracking.sendFailureEvent(t, "PUT Failure", f.toString,
                firehoseName, "snowplow-stream-shred", attemptNumber.toLong, putSize)
              case _ => None
            }

            Thread.sleep(backoffTime)
        }
      }
    }
  }

  private def multiPut(name: String, batch: List[(ByteBuffer, String)]): Future[PutRecordBatchResult] =
    Future {
      val putRecordsRequest = {
        val records: immutable.Seq[Record] = batch.map {
          case (b, _) => new Record().withData(b)
        }
        new PutRecordBatchRequest().withDeliveryStreamName(name).withRecords(records.asJavaCollection)
      }
      client.putRecordBatch(putRecordsRequest)
    }

  private[sinks] def getErrorsSummary(badResponses: List[PutRecordBatchResponseEntry]): Map[String, (Long, String)] = {
    badResponses.foldLeft(Map[String, (Long, String)]())((counts, r) => if (counts.contains(r.getErrorCode)) {
      counts + (r.getErrorCode -> (counts(r.getErrorCode)._1 + 1 -> r.getErrorMessage))
    } else {
      counts + (r.getErrorCode -> ((1, r.getErrorMessage)))
    })
  }

  private[sinks] def logErrorsSummary(errorsSummary: Map[String, (Long, String)]): Unit = {
    for ((errorCode, (count, sampleMessage)) <- errorsSummary) {
      log.error(s"$count records failed with error code ${errorCode}. Example error message: ${sampleMessage}")
    }
  }

  /**
    * How long to wait before sending the next request
    *
    * @param lastBackoff The previous backoff time
    * @return Minimum of maxBackoff and a random number between minBackoff and three times lastBackoff
    */
  private def getNextBackoff(lastBackoff: Long): Long = {
    val offset: Long = (randomGenerator.nextDouble() * (lastBackoff * 3 - minBackoff)).toLong
    val sum: Long = minBackoff + offset
    sum min maxBackoff
  }
}
