import DependecyVersions._

libraryDependencies ++= Seq(
  "org.apache.parquet" % "parquet-hadoop" % parquetVersion
    exclude(org = "org.slf4j", name = "slf4j-api"),
  "com.chuusai" %% "shapeless" % "2.3.4",
  "org.apache.hadoop" % "hadoop-client" % hadoopVersion % Provided,
  "org.slf4j" % "slf4j-api" % slf4jVersion,
  "org.scala-lang.modules" %% "scala-collection-compat" % "2.4.3",

  // tests
  "org.mockito" %% "mockito-scala-scalatest" % "1.16.37" % "test",
  "org.scalatest" %% "scalatest" % "3.2.7" % "test,it",
  "ch.qos.logback" % "logback-classic" % "1.2.3" % "test,it",
  "org.slf4j" % "log4j-over-slf4j" % slf4jVersion % "test,it",
  "com.google.guava" % "guava" % "30.1.1-jre" % "it"
)

lazy val sparkDeps = Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion % "it"
    exclude(org = "org.apache.hadoop", name = "hadoop-client")
    exclude(org = "org.slf4j", name = "slf4j-api")
    exclude(org = "org.apache.parquet", name = "parquet-hadoop"),
  "org.apache.spark" %% "spark-sql" % sparkVersion % "it"
    exclude(org = "org.apache.hadoop", name = "hadoop-client")
)

libraryDependencies ++= {
  val scala = scalaBinaryVersion.value
  scala match {
    case "2.11" | "2.12" => sparkDeps
    case _ => Seq.empty
  }
}

excludeDependencies ++= Seq(
  ExclusionRule("org.slf4j", "slf4j-log4j12")
)
