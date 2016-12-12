/*
 * Copyright (c) 2013-2014 Snowplow Analytics Ltd.
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
package com.snowplowanalytics
package snowplow.enrich
package kinesis
package sources

// Java
import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets.UTF_8
import java.util.UUID

import com.amazonaws.services.kinesis.model.Record
import com.fasterxml.jackson.databind.JsonNode
import com.snowplowanalytics.iglu.client.{ProcessingMessageNel, SchemaKey}
import com.snowplowanalytics.snowplow.enrich.common.{JsonSchemaPairs, ValidatedEnrichedEvent}
import com.snowplowanalytics.snowplow.enrich.common.loaders.{CollectorPayload, TsvLoader}
import org.slf4j.Logger

import scala.collection.immutable.Seq

// Amazon
import com.amazonaws.auth._

// Apache commons
import org.apache.commons.codec.binary.Base64

// Scala
import scala.util.Random
import scala.util.control.NonFatal

// Scalaz
import scalaz.{Sink => _, _}
import Scalaz._

// json4s
import org.json4s.scalaz.JsonScalaz._
import org.json4s.{ThreadLocal => _, _}
import org.json4s.JsonDSL._
import org.json4s.jackson.JsonMethods._

// Joda-Time
import org.joda.time.DateTime

// Iglu
import iglu.client.Resolver

// Snowplow
import sinks._
import common.outputs.{
EnrichedEvent,
BadRow
}
import common.loaders.ThriftLoader
import common.enrichments.EnrichmentRegistry

import common.ValidatedMaybeCollectorPayload
import common.EtlPipeline


// Tracker
import com.snowplowanalytics.snowplow.scalatracker.Tracker

object AbstractSource {

  /**
    * If a bad row JSON is too big, reduce it's size
    *
    * @param value Bad row JSON which is too large
    * @return Bad row JSON with `size` field instead of `line` field
    */
  def adjustOversizedFailureJson(value: String): String = {
    val size = getSize(value)
    try {

      val jsonWithoutLine = parse(value) removeField {
        case ("line", _) => true
        case _ => false
      }

      compact(render({
        ("size" -> size): JValue
      } merge jsonWithoutLine))

    } catch {
      case NonFatal(e) =>
        BadRow.oversizedRow(size, NonEmptyList("Unable to extract errors field from original oversized bad row JSON"))
    }
  }

  /**
    * Convert a too-large successful event to a failure
    *
    * @param value   Event which passed enrichment but was too large
    * @param maximum Maximum allowable bytes
    * @return Bad row JSON
    */
  def oversizedSuccessToFailure(value: String, maximum: Long): String = {
    val size = AbstractSource.getSize(value)
    BadRow.oversizedRow(size, NonEmptyList(s"Enriched event size of $size bytes is greater than allowed maximum of $maximum"))
  }

  /**
    * The size of a string in bytes
    *
    * @param evt
    * @return size
    */
  def getSize(evt: String): Long = ByteBuffer.wrap(evt.getBytes(UTF_8)).capacity
}

/**
  * Abstract base for the different sources
  * we support.
  */
abstract class AbstractSource(config: KinesisConfig, igluResolver: Resolver,
                              enrichmentRegistry: EnrichmentRegistry,
                              tracker: Option[Tracker]) {

  val log: Logger

  import log.{error, info, debug}

  val MaxRecordSize = if (config.sink == Sink.Kinesis) {
    Some(MaxBytes)
  } else {
    None
  }

  /**
    * Never-ending processing loop over source stream.
    */
  def run

  // Initialize a kinesis provider to use with a Kinesis source or sink.
  protected val kinesisProvider = config.credentialsProvider

  // Initialize the sink to output enriched events to.
  protected val enrichSink = getThreadLocalSink(InputType.EnrichGood)

  protected val enrichBadSink = getThreadLocalSink(InputType.EnrichBad)

  protected val shredSink = getThreadLocalSink(InputType.ShredGood)

  protected val shredBadSink = getThreadLocalSink(InputType.ShredBad)

  protected val firehoseSink = getThreadLocalSink(InputType.Firehose)

  protected val uaFirehoseSink = getThreadLocalSink(InputType.UAFirehose)

  protected val performanceFirehoseSink = getThreadLocalSink(InputType.PerformanceFirehose)

  private val UA_PARSER_CONTEXT = "ua_parser_context"
  private val PERFORMANCE_TIMING_CONTEXT = "PerformanceTiming"

  /**
    * We need the sink to be ThreadLocal as otherwise a single copy
    * will be shared between threads for different shards
    *
    * @param inputType Whether the sink is for good events or bad events
    * @return ThreadLocal sink
    */
  private def getThreadLocalSink(inputType: InputType.InputType) = new ThreadLocal[Option[ISink]] {
    override def initialValue = config.sink match {
      case Sink.Kafka => new KafkaSink(config, inputType, tracker).some
      case Sink.Kinesis => new KinesisSink(kinesisProvider, config, inputType, tracker).some
      case Sink.Stdouterr => new StdouterrSink(inputType).some
      case Sink.Test => None
      case Sink.Firehose => new FirehoseSink(kinesisProvider, config, inputType, tracker).some
      case Sink.UAFirehose => new FirehoseSink(kinesisProvider, config, InputType.UAFirehose, tracker).some
      case Sink.PerformanceFirehose => new FirehoseSink(kinesisProvider, config, InputType.PerformanceFirehose, tracker).some
    }
  }

  implicit val resolver: Resolver = igluResolver

  // Iterate through an enriched EnrichedEvent object and tab separate
  // the fields to a string.
  def tabSeparateEnrichedEvent(output: EnrichedEvent): String = {
    output.getClass.getDeclaredFields
      .map { field =>
        field.setAccessible(true)
        Option(field.get(output)).getOrElse("")
      }.mkString("\t")
  }

  /**
    * Convert incoming binary Thrift records to lists of enriched events
    *
    * @param binaryData Thrift raw event
    * @return List containing successful or failed events, each with a
    *         partition key
    */
  def enrichEvents(binaryData: Array[Byte]): List[Validation[(String, String), (String, String)]] = {
    val canonicalInput: ValidatedMaybeCollectorPayload = ThriftLoader.toCollectorPayload(binaryData)
    val processedEvents: List[ValidationNel[String, EnrichedEvent]] = EtlPipeline.processEvents(
      enrichmentRegistry,
      s"kinesis-${generated.Settings.version}",
      new DateTime(System.currentTimeMillis),
      canonicalInput)
    processedEvents.map(validatedMaybeEvent => {
      validatedMaybeEvent match {
        case Success(co) => (tabSeparateEnrichedEvent(co), if (config.useIpAddressAsPartitionKey) {
          co.user_ipaddress
        } else {
          UUID.randomUUID.toString
        }).success
        case Failure(errors) => {
          val line = new String(Base64.encodeBase64(binaryData), UTF_8)
          (BadRow(line, errors).toCompactJson -> Random.nextInt.toString).fail
        }
      }
    })
  }

  /**
    * Deserialize and enrich incoming Thrift records and store the results
    * in the appropriate sinks. If doing so causes the number of events
    * stored in a sink to become sufficiently large, all sinks are flushed
    * and we return `true`, signalling that it is time to checkpoint
    *
    * @param binaryData Thrift raw event
    * @return Whether to checkpoint
    */
  def enrichAndStoreEvents(binaryData: List[Array[Byte]]): Boolean = {
    val enrichedEvents = binaryData.flatMap(enrichEvents(_))
    val successes = enrichedEvents collect { case Success(s) => s }
    val sizeUnadjustedFailures = enrichedEvents collect { case Failure(s) => s }
    val failures = sizeUnadjustedFailures map {
      case (value, key) => if (!isTooLarge(value)) {
        value -> key
      } else {
        AbstractSource.adjustOversizedFailureJson(value) -> key
      }
    }

    val (tooBigSuccesses, smallEnoughSuccesses) = successes partition { s => isTooLarge(s._1) }

    val sizeBasedFailures = for {
      (value, key) <- tooBigSuccesses
      m <- MaxRecordSize
    } yield AbstractSource.oversizedSuccessToFailure(value, m) -> key

    val successesTriggeredFlush = enrichSink.get.map(_.storeEnrichedEvents(smallEnoughSuccesses))
    val failuresTriggeredFlush = enrichBadSink.get.map(_.storeEnrichedEvents(failures ++ sizeBasedFailures))
    error(s"Enrich errors: $failures")
    error(s"Enrich size based errors: $sizeBasedFailures")
    if (successesTriggeredFlush == Some(true) || failuresTriggeredFlush == Some(true)) {

      // Block until the records have been sent to Kinesis
      enrichSink.get.foreach(_.flush)
      enrichBadSink.get.foreach(_.flush)
      true
    } else {
      false
    }

  }

  /**
    * Shread means that we will retrieve all json fields from atomic.events, validate them and stored in derived topic.
    * The original atomic event will be updated to not include json fields.
    *
    * @param events
    * @return
    */
  def shredAndStoreEvents(events: List[String]): Boolean = {

    val common: Seq[ValidatedNel[(String, String, JsonSchemaPairs)]] = events.map { l: String =>
      ShredJob.loadAndShred(l)
    }

    val bad: Seq[ProcessingMessageNel] = common.flatMap { o: ValidatedNel[EventComponents] =>
      ShredJob.projectBads(o)
    }

    info(s"Derived Data errors: $bad")

    val errors = shredBadSink.get().map(_.storeEnrichedEvents(bad.map(t => (t.toString(), "1")).toList))

    val good: Seq[(String, String, JsonSchemaPairs)] = common.flatMap { o: ValidatedNel[EventComponents] =>
      ShredJob.projectGoods(o)
    }

    val goodPairs = good.flatMap(t => t._3).map(t => (t._2.toString, t._1.name)).toList
    info(s"Derived Data jsons: $goodPairs")
    val uaContextErrors = uaFirehoseSink.get().map(_.storeEnrichedEvents(goodPairs.filter(_._2 == UA_PARSER_CONTEXT)))
    val performanceContextErrors = performanceFirehoseSink.get().map(_.storeEnrichedEvents(goodPairs.filter(_._2 == PERFORMANCE_TIMING_CONTEXT)))
    if (uaContextErrors == Some(true) || performanceContextErrors == Some(true)) {
      uaFirehoseSink.get.foreach(_.flush())
      performanceFirehoseSink.get().foreach(_.flush())
      true
    } else {
      false
    }
  }

  def storeAlteredAtomicEvents(originalEvents: List[String]): Boolean = {

    val altered = originalEvents.map(ShredJob.alterEnrichedEvent)
    val alteredWithPartitionKey = altered.map {
      event =>
        (event, if (config.useIpAddressAsPartitionKey) {
          //event.user_ipaddress
          event.split("\t").toVector.drop(13).head
        } else {
          UUID.randomUUID.toString
        })
    }
    val (tooBigSuccesses, smallEnoughSuccesses) = alteredWithPartitionKey partition { s => isTooLarge(s._1) }

    val sizeBasedFailures = for {
      (value, key) <- tooBigSuccesses
      m <- MaxRecordSize
    } yield AbstractSource.oversizedSuccessToFailure(value, m) -> key

    val successesTriggeredFlush = firehoseSink.get.map(_.storeEnrichedEvents(smallEnoughSuccesses))
    if (sizeBasedFailures.nonEmpty) {
      error(s"Too large records: $sizeBasedFailures")
    }

    if (successesTriggeredFlush == Some(true)) {
      firehoseSink.get.foreach(_.flush())
      true
    } else {
      false
    }
  }

  /**
    * Whether a record is too large to send to Kinesis
    *
    * @param evt
    * @return boolean size decision
    */
  private def isTooLarge(evt: String): Boolean = MaxRecordSize match {
    case None => false
    case Some(m) => AbstractSource.getSize(evt) >= m
  }

}
