import DependecyVersions._

libraryDependencies ++= Seq(
  "co.fs2" %% "fs2-core" % fs2Version,
  "org.apache.hadoop" % "hadoop-client" % hadoopVersion % Provided
)

excludeDependencies ++= Seq(
  ExclusionRule("org.slf4j", "slf4j-log4j12")
)
