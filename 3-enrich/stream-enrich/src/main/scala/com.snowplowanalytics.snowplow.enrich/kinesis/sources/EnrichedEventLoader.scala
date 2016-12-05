package com.snowplowanalytics.snowplow.enrich.kinesis.sources

import java.util.UUID

import com.snowplowanalytics.snowplow.enrich.common.{ValidatedEnrichedEvent, ValidatedString}
import com.snowplowanalytics.snowplow.enrich.common.outputs.EnrichedEvent
import org.joda.time.DateTimeZone
import org.joda.time.format.DateTimeFormat

import scala.util.Try


// Scalaz
import scalaz._
import Scalaz._

// Snowplow Utils
import com.snowplowanalytics.util.Tap._

/**
  * Created by gciuloaica on 05/12/2016.
  */
object EnrichedEventLoader {

  private val RedshiftTstampFormat = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss.SSS").withZone(DateTimeZone.UTC)

  private val FieldCount = 108

  private object FieldIndexes {
    // 0-indexed
    val collectorTstamp = 3
    val eventId = 6
    val contexts = 52
    val unstructEvent = 58
    val derivedContexts = 122
    val eventFingerprint = 129
  }


  /**
    * Converts the source string into a
    * ValidatedEnrichedEvent. Note that
    * this loads the bare minimum required
    * for shredding - basically four fields.
    *
    * @param line A line of data to convert
    * @return either a set of validation
    *         Failures or a EnrichedEvent
    *         Success.
    */
  // TODO: potentially in the future this could be replaced by some
  // kind of Scalding pack()
  def toEnrichedEvent(line: String): ValidatedEnrichedEvent = {

    val fields = line.split("\t", -1).map(f => if (f == "") null else f)
    val len = fields.length
    if (len < FieldCount)
      return s"Line does not match Snowplow enriched event (expected ${FieldCount}+ fields; found $len)".failNel[EnrichedEvent]

    val event = new EnrichedEvent().tap { e =>
      e.contexts = fields(FieldIndexes.contexts)
      e.unstruct_event = fields(FieldIndexes.unstructEvent)

      // Backward compatibility with old TSVs without a derived_contexts field
      if (fields.size >= FieldIndexes.derivedContexts + 1) {
        e.derived_contexts = fields(FieldIndexes.derivedContexts)
      }

      // Backward compatibility with old TSVs without a event_fingerprint field
      if (fields.size >= FieldIndexes.eventFingerprint + 1) {
        e.event_fingerprint = fields(FieldIndexes.eventFingerprint)
      }
    }

    // Get and validate the event ID
    val eventId = validateUuid("event_id", fields(FieldIndexes.eventId))
    for (id <- eventId) {
      event.event_id = id
    }

    // Get and validate the collector timestamp
    val collectorTstamp = validateTimestamp("collector_tstamp", fields(FieldIndexes.collectorTstamp))
    for (tstamp <- collectorTstamp) {
      event.collector_tstamp = tstamp
    }

    (eventId.toValidationNel |@| collectorTstamp.toValidationNel) {
      (_, _) => event
    }
  }

  /**
    * Validates that the given field contains a valid UUID.
    *
    * @param field The name of the field being validated
    * @param str   The String hopefully containing a UUID
    * @return a Scalaz ValidatedString containing either
    *         the original String on Success, or an error
    *         String on Failure.
    */
  private def validateUuid(field: String, str: String): ValidatedString = {

    def check(s: String)(u: UUID): Boolean = (u != null && s == u.toString)

    val uuid = Try(UUID.fromString(str)).toOption.filter(check(str))
    uuid match {
      case Some(_) => str.success
      case None => s"Field [$field]: [$str] is not a valid UUID".fail
    }
  }


  /**
    * Validates that the given field contains a valid
    * (Redshift/Postgres-compatible) timestamp.
    *
    * @param field The name of the field being validated
    * @param str   The String hopefully containing a
    *              Redshift/PG-compatible timestamp
    * @return a Scalaz ValidatedString containing either
    *         the original String on Success, or an error
    *         String on Failure.
    */
  private def validateTimestamp(field: String, str: String): ValidatedString =
    try {
      val _ = RedshiftTstampFormat.parseDateTime(str)
      str.success
    } catch {
      case e: Throwable =>
        s"Field [$field]: [$str] is not in the expected Redshift/Postgres timestamp format".fail
    }


}
