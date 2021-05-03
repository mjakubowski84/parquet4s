import DependecyVersions._
import Releasing._
import bloop.integrations.sbt.BloopDefaults


lazy val twoEleven = "2.11.12"
lazy val twoTwelve = "2.12.13"
lazy val twoThirteen = "2.13.5"
lazy val supportedScalaVersions = Seq(twoEleven, twoTwelve, twoThirteen)
lazy val fs2ScalaVersions = Seq(twoTwelve, twoThirteen)
lazy val akkaScalaVersions = Seq(twoTwelve, twoThirteen)

ThisBuild / organization := "com.github.mjakubowski84"
ThisBuild / version := "2.0.0-SNAPSHOT"
ThisBuild / isSnapshot := true
ThisBuild / scalaVersion := twoThirteen
ThisBuild / scalacOptions ++= Seq("-deprecation", "-target:jvm-1.8")
ThisBuild / javacOptions ++= Seq("-source", "1.8", "-target", "1.8", "-unchecked", "-deprecation", "-feature")
ThisBuild / resolvers := Seq(
  Opts.resolver.sonatypeReleases,
  Resolver.jcenterRepo
)
ThisBuild / makePomConfiguration := makePomConfiguration.value.withConfigurations(Configurations.defaultMavenConfigurations)
Global / excludeLintKeys += run / cancelable
Global / excludeLintKeys += IntegrationTest / publishArtifact
Global / excludeLintKeys += makePomConfiguration


lazy val itSettings = Defaults.itSettings ++
  Project.inConfig(IntegrationTest)(Seq(
    fork := true,
    parallelExecution := false,
    testOptions += Tests.Argument("-u", "target/junit/" + scalaBinaryVersion.value)
  )) ++
  Project.inConfig(IntegrationTest)(BloopDefaults.configSettings)

lazy val testReportSettings = Project.inConfig(Test)(Seq(
  testOptions += Tests.Argument("-u", "target/junit/" + scalaBinaryVersion.value)
))

// used only for testing in core module
lazy val sparkDeps = Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion % "it"
    exclude(org = "org.apache.hadoop", name = "hadoop-client")
    exclude(org = "org.slf4j", name = "slf4j-api")
    exclude(org = "org.apache.parquet", name = "parquet-hadoop"),
  "org.apache.spark" %% "spark-sql" % sparkVersion % "it"
    exclude(org = "org.apache.hadoop", name = "hadoop-client")
)

lazy val core = (project in file("core"))
  .configs(IntegrationTest)
  .settings(
    name := "parquet4s-core",
    crossScalaVersions := supportedScalaVersions,
    libraryDependencies ++= Seq(
      "org.apache.parquet" % "parquet-hadoop" % parquetVersion
        exclude(org = "org.slf4j", name = "slf4j-api"),
      "com.chuusai" %% "shapeless" % shapelessVersion,
      "org.apache.hadoop" % "hadoop-client" % hadoopVersion % Provided,
      "org.slf4j" % "slf4j-api" % slf4jVersion,
      "org.scala-lang.modules" %% "scala-collection-compat" % "2.4.3",

      // tests
      "org.mockito" %% "mockito-scala-scalatest" % "1.16.37" % "test",
      "org.scalatest" %% "scalatest" % "3.2.7" % "test,it",
      "ch.qos.logback" % "logback-classic" % "1.2.3" % "test,it",
      "org.slf4j" % "log4j-over-slf4j" % slf4jVersion % "test,it"
    ) ++ {
      val scala = scalaBinaryVersion.value
      scala match {
        case "2.12" => sparkDeps
        case _ => Seq.empty
      }
    },
    excludeDependencies ++= Seq(
      ExclusionRule("org.slf4j", "slf4j-log4j12")
    )
  )
  .settings(itSettings)
  .settings(publishSettings)
  .settings(testReportSettings)

lazy val akka = (project in file("akka"))
  .configs(IntegrationTest)
  .settings(
    name := "parquet4s-akka",
    crossScalaVersions := akkaScalaVersions,
    libraryDependencies ++= Seq(
      "com.typesafe.akka" %% "akka-stream" % akkaVersion,
      "org.apache.hadoop" % "hadoop-client" % hadoopVersion % Provided
    ),
    excludeDependencies ++= Seq(
      ExclusionRule("org.slf4j", "slf4j-log4j12")
    )
  )
  .settings(itSettings)
  .settings(publishSettings)
  .settings(testReportSettings)
  .dependsOn(core % "compile->compile;it->it")

lazy val fs2 = (project in file("fs2"))
  .configs(IntegrationTest)
  .settings(
    name := "parquet4s-fs2",
    crossScalaVersions := fs2ScalaVersions,
    libraryDependencies ++= Seq(
      "co.fs2" %% "fs2-core" % fs2Version,
      "org.apache.hadoop" % "hadoop-client" % hadoopVersion % Provided,
      "co.fs2" %% "fs2-io" % fs2Version % "it",
      "org.typelevel" %% "cats-effect-testing-scalatest" % "1.1.0" % "it"
    ),
    excludeDependencies ++= Seq(
      ExclusionRule("org.slf4j", "slf4j-log4j12")
    )
  )
  .settings(itSettings)
  .settings(publishSettings)
  .settings(testReportSettings)
  .dependsOn(core % "compile->compile;it->it")

lazy val examples = (project in file("examples"))
  .settings(
    name := "parquet4s-examples",
    crossScalaVersions := fs2ScalaVersions,
    publish / skip := true,
    publishLocal / skip := true,
    libraryDependencies ++= Seq(
      "org.apache.hadoop" % "hadoop-client" % hadoopVersion,
      "io.github.embeddedkafka" %% "embedded-kafka" % "2.8.0",
      "ch.qos.logback" % "logback-classic" % "1.2.3",
      "org.slf4j" % "log4j-over-slf4j" % slf4jVersion,
      "com.typesafe.akka" %% "akka-stream-kafka" % "2.0.7",
      "com.github.fd4s" %% "fs2-kafka" % "2.0.0",
      "co.fs2" %% "fs2-io" % fs2Version
    ),
    excludeDependencies ++= Seq(
      ExclusionRule("org.slf4j", "slf4j-log4j12")
    ),
    run / cancelable := true,
    run / fork := true
  )
  .dependsOn(akka, fs2)

lazy val benchmarks = (project in file("benchmarks"))
  .settings(
    name := "parquet4s-benchmarks",
    publish / skip := true,
    publishLocal / skip := true,
    crossScalaVersions := Nil,
    libraryDependencies ++= Seq(
      "org.apache.hadoop" % "hadoop-client" % hadoopVersion,
      "com.storm-enroute" %% "scalameter" % "0.21",
      "org.slf4j" % "slf4j-nop" % slf4jVersion,
      "org.slf4j" % "log4j-over-slf4j" % slf4jVersion
    ),
    excludeDependencies ++= Seq(
      ExclusionRule("org.slf4j", "slf4j-log4j12")
    ),
    run / cancelable := true,
    run / fork := true
  )
  .dependsOn(akka, fs2)

lazy val root = (project in file("."))
  .settings(publishSettings)
  .settings(
    crossScalaVersions := Nil,
    publish / skip := true,
    publishLocal / skip := true
  )
  .aggregate(core, akka, fs2, examples)
