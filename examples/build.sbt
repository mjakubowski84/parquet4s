import DependecyVersions._

libraryDependencies ++= Seq(
  "org.apache.hadoop" % "hadoop-client" % hadoopVersion % Provided,
  "io.github.embeddedkafka" %% "embedded-kafka" % "2.2.0",
  "ch.qos.logback" % "logback-classic" % "1.2.3",
  "org.slf4j" % "log4j-over-slf4j" % slf4jVersion,
  "com.typesafe.akka" %% "akka-stream-kafka" % "1.0.3"
)

excludeDependencies ++= Seq(
  ExclusionRule("org.slf4j", "slf4j-log4j12")
)
