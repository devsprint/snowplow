/* 
 * Copyright (c) 2013-2014 Snowplow Analytics Ltd. All rights reserved.
 *
 * This program is licensed to you under the Apache License Version 2.0,
 * and you may not use this file except in compliance with the Apache License Version 2.0.
 * You may obtain a copy of the Apache License Version 2.0 at http://www.apache.org/licenses/LICENSE-2.0.
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the Apache License Version 2.0 is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the Apache License Version 2.0 for the specific language governing permissions and limitations there under.
 */

import sbt._
import Keys._

object SnowplowStreamEnrichBuild extends Build {

  import Dependencies._
  import BuildSettings._

  // Configure prompt to show current project
  override lazy val settings = super.settings :+ {
    shellPrompt := { s => Project.extract(s).currentProject.id + " > " }
  }

  lazy val root = Project("snowplow-stream-enrich-shred", file(".")).aggregate(common, projectEnrich, projectShred)

  lazy val common = Project("common", file("common"))
    .settings(basicSettings ++ scalifySettings: _*)
    .settings(
      libraryDependencies ++= Seq(
        Libraries.logging,
        Libraries.httpCore,
        Libraries.httpClient,
        Libraries.jacksonCore,
        Libraries.argot,
        Libraries.config,
        Libraries.scalaUtil,
        Libraries.snowplowRawEvent,
        Libraries.snowplowCommonEnrich,
        Libraries.scalazon excludeAll( ExclusionRule(organization = "com.amazonaws")),
        Libraries.scalaz7,
        Libraries.specs2,
        Libraries.scalazSpecs2,
        Libraries.commonsLang3,
        Libraries.thrift,
        Libraries.slf4j,
        Libraries.log4jOverSlf4j,
        Libraries.awsSdkKinesis,
        Libraries.kinesisClient,
        Libraries.igluClient,
        Libraries.snowplowTracker,
        Libraries.kafkaClients
        // Add your additional libraries here (comma-separated)...
      )
    )

  // Define our project, with basic project information and library dependencies
  lazy val projectEnrich = Project("snowplow-stream-enrich", file("enrich"))
    .settings(basicSettings ++ sbtAssemblySettings: _*)
    .dependsOn(common)

  lazy val projectShred = Project("snowplow-stream-shred", file("shred"))
    .settings(basicSettings ++ sbtAssemblySettings: _*)
    .dependsOn(common)
}
