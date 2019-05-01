import DependecyVersions._

libraryDependencies ++= Seq(
  "org.apache.parquet" % "parquet-hadoop" % parquetVersion
    exclude(org = "org.slf4j", name = "slf4j-api"),
  "com.chuusai" %% "shapeless" % "2.3.3",
  "org.apache.hadoop" % "hadoop-client" % hadoopVersion % Provided,
  "org.slf4j" % "slf4j-api" % slf4jVersion,

  // tests
  "org.scalamock" %% "scalamock" % "4.1.0" % "test",
  "org.scalatest" %% "scalatest" % "3.0.5" % "test,it",
  "org.apache.spark" %% "spark-core" % sparkVersion % "it"
    exclude(org = "org.apache.hadoop", name = "hadoop-client")
    exclude(org = "org.slf4j", name = "slf4j-api"),
  "org.apache.spark" %% "spark-sql" % sparkVersion % "it"
    exclude(org = "org.apache.hadoop", name = "hadoop-client"),
  "ch.qos.logback" % "logback-classic" % "1.2.3" % "test,it",
  "org.slf4j" % "log4j-over-slf4j" % slf4jVersion % "test,it"
)

excludeDependencies ++= Seq(
  ExclusionRule("org.slf4j", "slf4j-log4j12")
)
