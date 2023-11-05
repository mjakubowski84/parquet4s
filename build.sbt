import DependecyVersions._
import Releasing._
import Documentation._
import Compilation._
import bloop.integrations.sbt.BloopDefaults
import sbt.util

lazy val twoTwelve              = "2.12.18"
lazy val twoThirteen            = "2.13.12"
lazy val three                  = "3.3.1"
lazy val supportedScalaVersions = Seq(twoTwelve, twoThirteen, three)

val akkaLib  = ActorLibCross("-akka", "-akka")
val pekkoLib = ActorLibCross("-pekko", "-pekko")

ThisBuild / organization := "com.github.mjakubowski84"
ThisBuild / version := "2.14.0"
ThisBuild / isSnapshot := false
ThisBuild / scalaVersion := twoThirteen

ThisBuild / javacOptions ++= Seq("-source", "1.8", "-target", "1.8")
ThisBuild / resolvers := Opts.resolver.sonatypeOssReleases :+ Resolver.jcenterRepo

ThisBuild / makePomConfiguration := makePomConfiguration.value.withConfigurations(
  Configurations.defaultMavenConfigurations
)
ThisBuild / versionScheme := Some("semver-spec")
Global / excludeLintKeys ++= Set(
  run / cancelable,
  IntegrationTest / publishArtifact,
  makePomConfiguration,
  publish / parallelExecution,
  publishLocal / parallelExecution
)

lazy val itSettings = Defaults.itSettings ++
  Project.inConfig(IntegrationTest)(
    Seq(
      fork := true,
      parallelExecution := false,
      testOptions += Tests.Argument("-u", "target/junit/" + scalaBinaryVersion.value)
    )
  ) ++
  Project.inConfig(IntegrationTest)(BloopDefaults.configSettings)

lazy val testReportSettings = Project.inConfig(Test)(
  Seq(
    testOptions += Tests.Argument("-u", "target/junit/" + scalaBinaryVersion.value)
  )
)

// used only for testing in core module
lazy val sparkDeps = Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion % "it"
    exclude (org = "org.apache.hadoop", name = "hadoop-client")
    exclude (org = "org.apache.hadoop", name = "hadoop-client-api")
    exclude (org = "org.slf4j", name = "slf4j-api")
    exclude (org = "org.apache.parquet", name = "parquet-hadoop"),
  "org.apache.spark" %% "spark-sql" % sparkVersion % "it"
    exclude (org = "org.apache.hadoop", name = "hadoop-client")
)

lazy val core = (projectMatrix in file("core"))
  .configs(IntegrationTest)
  .settings(
    name := "parquet4s-core",
    libraryDependencies ++= Seq(
      "org.apache.parquet" % "parquet-hadoop" % parquetVersion
        exclude (org = "org.slf4j", name = "slf4j-api"),
      "org.apache.hadoop" % "hadoop-client" % hadoopVersion % Provided,
      "org.slf4j" % "slf4j-api" % slf4jVersion,
      "org.scala-lang.modules" %% "scala-collection-compat" % scalaCollectionCompatVersion,
      // tests
      "org.mockito" % "mockito-core" % mockitoVersion % "test",
      "org.scalatest" %% "scalatest" % scalatestVersion % "test,it",
      "ch.qos.logback" % "logback-classic" % logbackVersion % "test,it",
      "org.slf4j" % "log4j-over-slf4j" % slf4jVersion % "test,it"
    ),
    excludeDependencies ++= Seq(
      ExclusionRule("org.slf4j", "slf4j-log4j12")
    )
  )
  .jvmPlatform(
    scalaVersions = Seq(twoTwelve, twoThirteen),
    settings = Def.settings(
      libraryDependencies ++= sparkDeps :+ ("com.chuusai" %% "shapeless" % shapelessVersion)
    )
  )
  .jvmPlatform(
    scalaVersions = Seq(three)
  )
  .settings(compilationSettings)
  .settings(itSettings)
  .settings(publishSettings)
  .settings(testReportSettings)
  .dependsOn(testkit % "it->compile")

lazy val akkaPekko = (projectMatrix in file("akkaPekko"))
  .configs(IntegrationTest)
  .settings(
    libraryDependencies ++= Seq(
      "org.apache.hadoop" % "hadoop-client" % hadoopVersion % Provided
    ),
    excludeDependencies ++= Seq(
      ExclusionRule("org.slf4j", "slf4j-log4j12")
    )
  )
  .jvmPlatform(
    scalaVersions = supportedScalaVersions,
    axisValues    = Seq(akkaLib),
    settings = Def.settings(
      name := "parquet4s-akka",
      libraryDependencies ++= Seq(
        "com.typesafe.akka" %% "akka-actor" % akkaVersion,
        "com.typesafe.akka" %% "akka-stream" % akkaVersion
      )
    )
  )
  .jvmPlatform(
    scalaVersions = supportedScalaVersions,
    axisValues    = Seq(pekkoLib),
    settings = Def.settings(
      name := "parquet4s-pekko",
      libraryDependencies ++= Seq(
        "org.apache.pekko" %% "pekko-actor" % pekkoVersion,
        "org.apache.pekko" %% "pekko-stream" % pekkoVersion
      )
    )
  )
  .settings(compilationSettings)
  .settings(itSettings)
  .settings(publishSettings)
  .settings(testReportSettings)
  .dependsOn(core % "compile->compile;it->it")

lazy val fs2 = (projectMatrix in file("fs2"))
  .configs(IntegrationTest)
  .settings(
    name := "parquet4s-fs2",
    libraryDependencies ++= Seq(
      "co.fs2" %% "fs2-core" % fs2Version,
      "org.typelevel" %% "cats-effect" % catsEffectVersion,
      "org.apache.hadoop" % "hadoop-client" % hadoopVersion % Provided,
      "co.fs2" %% "fs2-io" % fs2Version % "it",
      "org.typelevel" %% "cats-effect-testing-scalatest" % "1.5.0" % "it"
    ),
    excludeDependencies ++= Seq(
      ExclusionRule("org.slf4j", "slf4j-log4j12")
    )
  )
  .jvmPlatform(
    scalaVersions = supportedScalaVersions
  )
  .settings(compilationSettings)
  .settings(itSettings)
  .settings(publishSettings)
  .settings(testReportSettings)
  .dependsOn(core % "compile->compile;test->test", testkit % "it->compile")

lazy val scalaPB = (projectMatrix in file("scalapb"))
  .configs(IntegrationTest)
  .settings(
    libraryDependencies ++= Seq(
      "com.thesamet.scalapb" %% "scalapb-runtime" % scalapb.compiler.Version.scalapbVersion,
      "org.apache.hadoop" % "hadoop-client" % hadoopVersion % Test,
      "org.apache.parquet" % "parquet-protobuf" % parquetVersion % Test,
      "org.typelevel" %% "cats-effect-testing-scalatest" % "1.5.0" % Test
    ),
    compileOrder := CompileOrder.JavaThenScala,
    Test / PB.targets := Seq(
      scalapb.gen(flatPackage = true, lenses = false) -> ((Test / sourceManaged).value / "protobuf/scala"),
      PB.gens.java -> ((Test / sourceManaged).value / "protobuf/java")
    )
  )
  .jvmPlatform(
    scalaVersions = supportedScalaVersions,
    axisValues    = Seq(akkaLib),
    settings = Def.settings(
      name := "parquet4s-scalapb-akka"
    )
  )
  .jvmPlatform(
    scalaVersions = supportedScalaVersions,
    axisValues    = Seq(pekkoLib),
    settings = Def.settings(
      name := "parquet4s-scalapb-pekko"
    )
  )
  .settings(compilationSettings)
  .settings(itSettings)
  .settings(publishSettings)
  .settings(testReportSettings)
  .dependsOn(core % "compile->compile;test->test", akkaPekko % "test->compile", fs2 % "test->compile")

lazy val testkit = (projectMatrix in file("testkit"))
  .settings(
    name := "parquet4s-testkit",
    publish / skip := true,
    publishLocal / skip := true,
    libraryDependencies ++= Seq(
      "org.scalatest" %% "scalatest" % scalatestVersion,
      "org.apache.hadoop" % "hadoop-minicluster" % hadoopVersion,
      // MiniDFSCluster leaks Mockito via NameNodeAdapter while hadoop-minicluster doesn't bring
      // the dependency transitively. We have to add it explicitly to prevent ClassNotFoundException.
      "org.mockito" % "mockito-core" % mockitoVersion,
      "org.slf4j" % "log4j-over-slf4j" % slf4jVersion,
      "ch.qos.logback" % "logback-classic" % logbackVersion
    )
  )
  .jvmPlatform(
    scalaVersions = supportedScalaVersions
  )

lazy val examples = (projectMatrix in file("examples"))
  .settings(
    libraryDependencies ++= Seq(
      "org.apache.hadoop" % "hadoop-client" % hadoopVersion,
      "org.apache.parquet" % "parquet-protobuf" % parquetVersion,
      "io.github.embeddedkafka" %% "embedded-kafka" % "3.6.0",
      "ch.qos.logback" % "logback-classic" % logbackVersion,
      "org.slf4j" % "log4j-over-slf4j" % slf4jVersion,
      "com.github.fd4s" %% "fs2-kafka" % "3.2.0",
      "co.fs2" %% "fs2-io" % fs2Version
    ),
    excludeDependencies ++= Seq(
      ExclusionRule("org.slf4j", "slf4j-log4j12")
    ),
    publish / skip := true,
    publishLocal / skip := true,
    evictionErrorLevel := util.Level.Warn,
    run / cancelable := true,
    run / fork := true,
    compileOrder := CompileOrder.JavaThenScala,
    Compile / PB.targets := Seq(
      scalapb.gen(flatPackage = true, lenses = false) -> ((Compile / sourceManaged).value / "protobuf/scala"),
      PB.gens.java -> ((Compile / sourceManaged).value / "protobuf/java")
    )
  )
  .jvmPlatform(
    scalaVersions = Seq(twoTwelve, twoThirteen),
    axisValues    = Seq(akkaLib),
    settings = Def.settings(
      name := "parquet4s-examples-akka",
      libraryDependencies ++= Seq(
        "com.typesafe.akka" %% "akka-stream-kafka" % {
          if (scalaVersion.value == twoThirteen) {
            "3.0.1"
          } // non-licensed version
          else {
            "2.1.1"
          }
        }
      )
    )
  )
  .jvmPlatform(
    scalaVersions = Seq(twoTwelve, twoThirteen),
    axisValues    = Seq(pekkoLib),
    settings = Def.settings(
      name := "parquet4s-examples-pekko",
      libraryDependencies ++= Seq("org.apache.pekko" %% "pekko-connectors-kafka" % "1.0.0")
    )
  )
  .settings(compilationSettings)
  .dependsOn(akkaPekko, fs2, scalaPB)

lazy val coreBenchmarks = (projectMatrix in file("coreBenchmarks"))
  .settings(
    name := "parquet4s-core-benchmarks",
    publish / skip := true,
    publishLocal / skip := true,
    libraryDependencies ++= Seq(
      "org.apache.hadoop" % "hadoop-client" % hadoopVersion,
      "org.slf4j" % "slf4j-nop" % slf4jVersion,
      "org.slf4j" % "log4j-over-slf4j" % slf4jVersion
    ),
    excludeDependencies ++= Seq(
      ExclusionRule("org.slf4j", "slf4j-log4j12")
    ),
    run / cancelable := true,
    run / fork := true
  )
  .jvmPlatform(
    scalaVersions = supportedScalaVersions
  )
  .settings(compilationSettings)
  .enablePlugins(JmhPlugin)
  .dependsOn(core)

lazy val akkaPekkoBenchmarks = (projectMatrix in file("akkaPekkoBenchmarks"))
  .settings(
    publish / skip := true,
    publishLocal / skip := true,
    libraryDependencies ++= Seq(
      "org.apache.hadoop" % "hadoop-client" % hadoopVersion,
      "org.slf4j" % "slf4j-nop" % slf4jVersion,
      "org.slf4j" % "log4j-over-slf4j" % slf4jVersion
    ),
    excludeDependencies ++= Seq(
      ExclusionRule("org.slf4j", "slf4j-log4j12")
    ),
    run / cancelable := true,
    run / fork := true
  )
  .jvmPlatform(
    scalaVersions = supportedScalaVersions,
    axisValues    = Seq(akkaLib),
    settings = Def.settings(
      name := "parquet4s-akka-benchmarks"
    )
  )
  .jvmPlatform(
    scalaVersions = supportedScalaVersions,
    axisValues    = Seq(pekkoLib),
    settings = Def.settings(
      name := "parquet4s-pekko-benchmarks"
    )
  )
  .settings(compilationSettings)
  .enablePlugins(JmhPlugin)
  .dependsOn(akkaPekko)

lazy val fs2Benchmarks = (projectMatrix in file("fs2Benchmarks"))
  .settings(
    name := "parquet4s-fs2-benchmarks",
    publish / skip := true,
    publishLocal / skip := true,
    libraryDependencies ++= Seq(
      "org.apache.hadoop" % "hadoop-client" % hadoopVersion,
      "org.slf4j" % "slf4j-nop" % slf4jVersion,
      "org.slf4j" % "log4j-over-slf4j" % slf4jVersion
    ),
    excludeDependencies ++= Seq(
      ExclusionRule("org.slf4j", "slf4j-log4j12")
    ),
    run / cancelable := true,
    run / fork := true
  )
  .jvmPlatform(
    scalaVersions = supportedScalaVersions
  )
  .settings(compilationSettings)
  .enablePlugins(JmhPlugin)
  .dependsOn(fs2)

lazy val documentation = (projectMatrix in file("site"))
  .settings(documentationSettings)
  .settings(
    publish / skip := true,
    libraryDependencies ++= Seq(
      "org.scalameta" %% "mdoc" % "2.4.0",
      "org.apache.hadoop" % "hadoop-client" % hadoopVersion,
      "org.slf4j" % "slf4j-nop" % slf4jVersion,
      "org.slf4j" % "log4j-over-slf4j" % slf4jVersion
    ),
    excludeDependencies ++= Seq(
      ExclusionRule("org.scala-lang.modules", "scala-collection-compat_2.13")
    ),
    dependencyOverrides ++= Seq(
      "org.scala-lang.modules" %% "scala-collection-compat" % scalaCollectionCompatVersion
    )
  )
  .dependsOn(core, akkaPekko, fs2, scalaPB)
  .enablePlugins(MicrositesPlugin)

lazy val root = (projectMatrix in file("."))
  .settings(
    crossScalaVersions := Nil,
    publish / skip := true,
    publish / parallelExecution := false,
    publishLocal / skip := true,
    publishLocal / parallelExecution := false
  )
  .aggregate(
    core,
    akkaPekko,
    fs2,
    scalaPB,
    testkit,
    examples,
    coreBenchmarks,
    akkaPekkoBenchmarks,
    fs2Benchmarks,
    documentation
  )

Compile / sources := Nil
Test / sources := Nil
publish / skip := true
