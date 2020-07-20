import DependecyVersions._

libraryDependencies ++= Seq(
  "org.apache.hadoop" % "hadoop-client" % hadoopVersion,
  "io.github.embeddedkafka" %% "embedded-kafka" % "2.6.0",
  "ch.qos.logback" % "logback-classic" % "1.2.3",
  "org.slf4j" % "log4j-over-slf4j" % slf4jVersion,
  "com.typesafe.akka" %% "akka-stream-kafka" % "2.0.4",
  "com.google.guava" % "guava" % "29.0-jre",
  "com.github.fd4s" %% "fs2-kafka" % "1.0.0",
  "co.fs2" %% "fs2-io" % fs2Version
)

excludeDependencies ++= Seq(
  ExclusionRule("org.slf4j", "slf4j-log4j12")
)
