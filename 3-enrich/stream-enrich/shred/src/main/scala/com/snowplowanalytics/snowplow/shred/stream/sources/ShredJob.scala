package com.snowplowanalytics.snowplow.shred.stream.sources

import java.io.{PrintWriter, StringWriter}
import java.util.UUID

import com.snowplowanalytics.iglu.client.validation.ProcessingMessageMethods._
import com.snowplowanalytics.iglu.client.{JsonSchemaPair, ProcessingMessageNel, Resolver}
import com.snowplowanalytics.snowplow.enrich.common.ValidatedNelMessage
import com.snowplowanalytics.snowplow.enrich.common.outputs.EnrichedEvent
import com.snowplowanalytics.snowplow.enrich.common.utils.ConversionUtils
import com.snowplowanalytics.snowplow.enrich.common.utils.shredder.Shredder

import scala.util.control.NonFatal


// Scalaz
import scalaz._

/**
  * shread job inspired from hadoop based implementation of ShredJob.
  */
object ShredJob {

  /** Convenient for passing around the parts of an event. */
  type EventComponents = Tuple4[String, String, List[JsonSchemaPair], String]

  /**
    * Pipelines our loading of raw lines into
    * shredding the JSONs.
    *
    * @param line The incoming raw line (hopefully
    *        holding a Snowplow enriched event)
    * @param resolver Our implicit Iglu
    *        Resolver, for schema lookups
    * @return a Validation boxing either a Nel of
    *         ProcessingMessages on Failure, or a
    *         (possibly empty) List of JSON instances
    *         + schemas on Success
    */
  def loadAndShred(line: String)(implicit resolver: Resolver): ValidatedNelMessage[EventComponents] =
    try {
      for {
        event <- EnrichedEventLoader.toEnrichedEvent(line).toProcessingMessages
        fp     = getEventFingerprint(event)
        shred <- Shredder.shred(event)
      } yield (event.event_id, fp, shred, event.etl_tstamp)
    } catch {
      case NonFatal(nf) =>
        val errorWriter = new StringWriter
        nf.printStackTrace(new PrintWriter(errorWriter))
        Validation.failure[String, EventComponents](
          s"Unexpected error processing events: $errorWriter").toProcessingMessageNel
    }



  /**
    * Projects our Failures into a Some; Successes
    * become a None will be silently dropped by
    * Scalding in this pipeline.
    *
    * @param all The Validation containing either
    *        our Successes or our Failures
    * @return an Option boxing either our List of
    *         Processing Messages on Failure, or
    *         None on Success
    */
  def projectBads(
                   all: ValidatedNelMessage[EventComponents]
                 ): Option[ProcessingMessageNel] =
    all.fold(
      e => Some((e)),
      _ => None // Discard
    )

  /**
    * Projects our Successes into a
    * Some; everything else will be silently
    * dropped by Scalding in this pipeline. Note
    * that no contexts still counts as a Success
    * (as we want to copy atomic-events even if
    * no shredding was needed).
    *
    * @param all The Validation containing either
    *        our Successes or our Failures
    * @return an Option boxing either our List of
    *         Processing Messages on Failure, or
    *         None on Success
    */
  def projectGoods(all: ValidatedNelMessage[EventComponents]): Option[EventComponents] =
    all.toOption

  // Indexes for the contexts, unstruct_event, and derived_contexts fields
  private val IgnoredJsonFields = Set(52, 58, 122)

  /**
    * Ready the enriched event for database load by removing JSON fields
    * and truncating field lengths based on Postgres' column types
    *
    * @param enrichedEvent TSV
    * @return the same TSV with the JSON fields removed
    */
  def alterEnrichedEvent(enrichedEvent: String): String = {

    // TODO: move PostgresConstraints code out into Postgres-specific shredder when ready.
    // It's okay to apply Postgres constraints to events being loaded into Redshift as the PG
    // constraints are typically more permissive, but Redshift will be protected by the
    // COPY ... TRUNCATECOLUMNS.
    (enrichedEvent.split("\t", -1).toList.zipAll(PostgresConstraints.maxFieldLengths, "", None))
      .map { case (field, maxLength) =>
        maxLength match {
          case Some(ml) => ConversionUtils.truncate(field, ml)
          case None => field
        }
      }
      .zipWithIndex
      .filter(x => ! ShredJob.IgnoredJsonFields.contains(x._2))
      .map(_._1)
      .mkString("\t")
  }

  /**
    * Retrieves the event fingerprint. IF the field is null then we
    * assign a UUID at random for the fingerprint instead. This
    * is to respect any deduplication which requires both event ID and
    * event fingerprint to match.
    *
    * @param event The event to extract a fingerprint from
    * @return the event fingerprint
    */
  private def getEventFingerprint(event: EnrichedEvent): String =
    Option(event.event_fingerprint).getOrElse(UUID.randomUUID().toString)


}
