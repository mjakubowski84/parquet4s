import DependecyVersions._

libraryDependencies ++= Seq(
  "org.apache.hadoop" % "hadoop-client" % hadoopVersion,
  "io.github.embeddedkafka" %% "embedded-kafka" % "2.3.1",
  "ch.qos.logback" % "logback-classic" % "1.2.3",
  "org.slf4j" % "log4j-over-slf4j" % slf4jVersion,
  "com.typesafe.akka" %% "akka-stream-kafka" % "1.1.0",
  "com.google.guava" % "guava" % "28.0-jre"
)

excludeDependencies ++= Seq(
  ExclusionRule("org.slf4j", "slf4j-log4j12")
)
