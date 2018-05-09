package com.snowplowanalytics.snowplow.shred.stream.sinks


object FirehoseDeliveryStreams {

  val Firehose = "snowplow-events"
  val UAFirehose = "snowplow-ua-parser-carnac"
  val PerformanceFirehose = "snowplow-performance-carnac"
  val GoogleAnalyticsCookiesFirehose = "snowplow-google-analytics-cookies-carnac"
  val GoogleAnalyticsSocialFirehose = "snowplow-google-analytics-social-carnac"
  val GoogleAnalyticsProductImpressionFirehose = "snowplow-google-analytics-prod-impression"
  val GoogleAnalyticsTransactionFirehose = "snowplow-google-analytics-transaction"
  val GoogleAnalyticsEecomActionFieldFirehose = "enhanced-ecommerce-action-field"
  val GoogleAnalyticsEecomActionFirehose = "enhanced-ecommerce-action"
  val GoogleAnalyticsEecomImpressionFieldFirehose = "enhanced-ecommerce-impression-field"
  val GoogleAnalyticsEecomProductFieldFirehose = "enhanced-ecommerce-product-field"
  val GoogleAnalyticsEecomPromoFieldFirehose = "enhanced-ecommerce-promo-field"

}
