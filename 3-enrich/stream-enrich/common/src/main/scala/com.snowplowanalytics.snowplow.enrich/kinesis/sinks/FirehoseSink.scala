/*
 * Copyright (c) 2013-2016 Snowplow Analytics Ltd.
 * All rights reserved.
 *
 * This program is licensed to you under the Apache License Version 2.0,
 * and you may not use this file except in compliance with the Apache
 * License Version 2.0.
 * You may obtain a copy of the Apache License Version 2.0 at
 * http://www.apache.org/licenses/LICENSE-2.0.
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the Apache License Version 2.0 is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied.
 *
 * See the Apache License Version 2.0 for the specific language
 * governing permissions and limitations there under.
 */
package com.snowplowanalytics.snowplow.enrich
package kinesis
package sinks


import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets.UTF_8

import com.amazonaws.auth.AWSCredentialsProvider
import com.amazonaws.services.kinesisfirehose.AmazonKinesisFirehoseClient
import com.amazonaws.services.kinesisfirehose.model._
import com.snowplowanalytics.snowplow.scalatracker.Tracker
import org.slf4j.LoggerFactory

import scala.util.control.NonFatal
import scala.collection.JavaConverters._

/**
  * Push the data to Firehose Delivery stream.
  */
class FirehoseSink(provider: AWSCredentialsProvider, config: KinesisConfig,
                   inputType: InputType.InputType, tracker: Option[Tracker]) extends ISink {

  private lazy val log = LoggerFactory.getLogger(getClass())

  import log.{error, debug, info, trace}

  private val name = inputType match {
    case InputType.Firehose => Some("snowplow")
    case InputType.UAFirehose => Some("snowplow-ua-paarser")
    case InputType.PerformanceFirehose => Some("snowplow-performance")
    case _ => None
  }

  private val maxBackoff = config.maxBackoff
  private val minBackoff = config.minBackoff
  private val randomGenerator = new java.util.Random()

  val client = name.map { firehoseName =>
    val firehose = new AmazonKinesisFirehoseClient(provider)
    val deliveryStreamRequest = new DescribeDeliveryStreamRequest()
      .withDeliveryStreamName(firehoseName)

    val response = firehose.describeDeliveryStream(deliveryStreamRequest)
    info(s"Firehose describe response: ${response}")
    val status = response.getDeliveryStreamDescription.getDeliveryStreamStatus
    if (status.toLowerCase != "active") {
      error(s"Cannot write because firehose delivery stream with name $name  is not active")
      System.exit(1)
      throw new RuntimeException("System.exit should never fail")
    }

    firehose
  }

  val ByteThreshold = config.byteLimit
  val RecordThreshold = config.recordLimit
  val TimeThreshold = config.timeLimit
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
    def sealBatch() {
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
    def addEvent(event: (ByteBuffer, String)) {
      val newBytes = event._1.capacity

      if (newBytes >= MaxBytes) {
        val original = new String(event._1.array, UTF_8)
        error(s"Dropping record with size $newBytes bytes: [$original]")
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
    def clear() {
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
    */
  override def storeEnrichedEvents(events: List[(String, String)]): Boolean = {
    debug(s"Trying to store events to firehose: \n $events")
    val wrappedEvents = events.map(e => ByteBuffer.wrap(s"${e._1}\n".getBytes(UTF_8)) -> e._2)
    wrappedEvents.foreach(FirehoseEventStorage.addEvent(_))

    // Log BadRows
    inputType match {
      case InputType.EnrichGood => None
      case InputType.EnrichBad => events.foreach(e => debug(s"BadRow: ${e._1}"))
      case InputType.ShredGood => None
      case InputType.ShredBad => events.foreach(e => debug(s"BadRow: ${e._1}"))
      case InputType.Firehose => None
    }

    if (FirehoseEventStorage.currentBatch.nonEmpty && System.currentTimeMillis() > nextRequestTime) {
      nextRequestTime = System.currentTimeMillis() + TimeThreshold
      true
    } else FirehoseEventStorage.completeBatches.nonEmpty
  }

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
  def sendBatch(batch: List[(ByteBuffer, String)]) {
    client.foreach { firehoseClient =>
      if (!batch.isEmpty) {
        info(s"Writing ${batch.size} records to Kinesis Firehose $name")
        var unsentRecords = batch
        var backoffTime = minBackoff
        var sentBatchSuccessfully = false
        var attemptNumber = 0
        while (!sentBatchSuccessfully) {
          attemptNumber += 1

          val firehoseRecords = unsentRecords.map { r =>
            new Record().withData(r._1)
          }

          val firehoseBatch =
            new PutRecordBatchRequest().withDeliveryStreamName(name.get).withRecords(firehoseRecords.asJava)

          try {
            //TODO: need to make it async.
            val results = firehoseClient.putRecordBatch(firehoseBatch).getRequestResponses.asScala.toList
            val failurePairs = unsentRecords zip results filter {
              _._2.getErrorMessage != null
            }
            info(s"Successfully wrote ${unsentRecords.size - failurePairs.size} out of ${unsentRecords.size} records")
            if (failurePairs.nonEmpty) {
              val (failedRecords, failedResults) = failurePairs.unzip
              unsentRecords = failedRecords
              logErrorsSummary(getErrorsSummary(failedResults))
              backoffTime = getNextBackoff(backoffTime)
              error(s"Retrying all failed records in $backoffTime milliseconds...")

              val err = s"Failed to send ${failurePairs.size} events"
              val putSize: Long = unsentRecords.foldLeft(0)((a, b) => a + b._1.capacity)

              tracker match {
                case Some(t) => SnowplowTracking.sendFailureEvent(t, "PUT Failure", err, name.get, "snowplow-stream-shred", attemptNumber, putSize)
                case _ => None
              }

              Thread.sleep(backoffTime)
            } else {
              sentBatchSuccessfully = true
            }
          } catch {
            case NonFatal(f) => {
              backoffTime = getNextBackoff(backoffTime)
              error(s"Writing failed.", f)
              error(s"  + Retrying in $backoffTime milliseconds...")

              val putSize: Long = unsentRecords.foldLeft(0)((a, b) => a + b._1.capacity)

              tracker match {
                case Some(t) => SnowplowTracking.sendFailureEvent(t, "PUT Failure", f.toString, name.get, "snowplow-stream-shred", attemptNumber, putSize)
                case _ => None
              }

              Thread.sleep(backoffTime)
            }
          }
        }
      }
    }
  }


  private[sinks] def getErrorsSummary(badResponses: List[PutRecordBatchResponseEntry]): Map[String, (Long, String)] = {
    badResponses.foldLeft(Map[String, (Long, String)]())((counts, r) => if (counts.contains(r.getErrorCode)) {
      counts + (r.getErrorCode -> (counts(r.getErrorCode)._1 + 1 -> r.getErrorMessage))
    } else {
      counts + (r.getErrorCode -> (1, r.getErrorMessage))
    })
  }

  private[sinks] def logErrorsSummary(errorsSummary: Map[String, (Long, String)]): Unit = {
    for ((errorCode, (count, sampleMessage)) <- errorsSummary) {
      error(s"$count records failed with error code ${errorCode}. Example error message: ${sampleMessage}")
    }
  }

  /**
    * How long to wait before sending the next request
    *
    * @param lastBackoff The previous backoff time
    * @return Minimum of maxBackoff and a random number between minBackoff and three times lastBackoff
    */
  private def getNextBackoff(lastBackoff: Long): Long = (minBackoff + randomGenerator.nextDouble() * (lastBackoff * 3 - minBackoff)).toLong.min(maxBackoff)
}
