name := "goober-spark"

version := "1.0"

scalaVersion := "2.10.4"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "1.4.0" % "provided",
  "org.apache.spark" %% "spark-sql" % "1.4.0" % "provided",
  "org.apache.spark" % "spark-streaming_2.10" % "1.4.0" % "provided",
  "org.apache.spark" % "spark-streaming-kafka_2.10" % "1.4.0",
  "net.debasishg" % "redisclient_2.10" % "3.0",
  "com.tdunning" % "t-digest" % "3.1",
  "com.typesafe.play" % "play-json_2.10" % "2.3.10",
  "org.scalanlp" % "breeze-math_2.10" % "0.4",
  "com.livestream" %% "scredis" % "2.0.6"
)

assemblyMergeStrategy in assembly := {
  case m if m.toLowerCase.endsWith("manifest.mf")          => MergeStrategy.discard
  case m if m.toLowerCase.matches("meta-inf.*\\.sf$")      => MergeStrategy.discard
  case "log4j.properties"                                  => MergeStrategy.discard
  case m if m.toLowerCase.startsWith("meta-inf/services/") => MergeStrategy.filterDistinctLines
  case "reference.conf"                                    => MergeStrategy.concat
  case _                                                   => MergeStrategy.first
}