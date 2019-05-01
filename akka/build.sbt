import DependecyVersions._

libraryDependencies ++= Seq(
  "com.typesafe.akka" %% "akka-stream" % akkaVersion,
  "org.apache.hadoop" % "hadoop-client" % hadoopVersion % Provided,
)

excludeDependencies ++= Seq(
  ExclusionRule("org.slf4j", "slf4j-log4j12")
)
