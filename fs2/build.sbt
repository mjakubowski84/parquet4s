import DependecyVersions._

libraryDependencies ++= Seq(
  "co.fs2" %% "fs2-core" % fs2Version,
  "org.apache.hadoop" % "hadoop-client" % hadoopVersion % Provided,
  "co.fs2" %% "fs2-io" % fs2Version % "it"
)

excludeDependencies ++= Seq(
  ExclusionRule("org.slf4j", "slf4j-log4j12")
)
