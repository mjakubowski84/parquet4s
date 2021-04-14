import bloop.integrations.sbt.BloopDefaults


lazy val supportedScalaVersions = Seq("2.11.12", "2.12.13", "2.13.5")
lazy val fs2ScalaVersions = Seq("2.12.13", "2.13.5")

ThisBuild / organization := "com.github.mjakubowski84"
ThisBuild / version := "1.9.0-SNAPSHOT"
ThisBuild / isSnapshot := true
ThisBuild / scalaVersion := "2.12.13"
ThisBuild / scalacOptions ++= Seq("-deprecation", "-target:jvm-1.8")
ThisBuild / javacOptions ++= Seq("-source", "1.8", "-target", "1.8", "-unchecked", "-deprecation", "-feature")
ThisBuild / resolvers := Seq(
  Opts.resolver.sonatypeReleases,
  Resolver.jcenterRepo
)
ThisBuild / makePomConfiguration := makePomConfiguration.value.withConfigurations(Configurations.defaultMavenConfigurations)


lazy val publishSettings = {
  import xerial.sbt.Sonatype._
  Seq(
    Keys.credentials ++= Seq(
      Credentials(
        realm = "Sonatype Nexus Repository Manager",
        host = "oss.sonatype.org",
        userName = sys.env.getOrElse(
          "SONATYPE_USER_NAME",
          {
            streams.value.log.warn("Undefined environment variable: SONATYPE_USER_NAME")
            "UNDEFINED"
          }
        ),
        passwd = sys.env.getOrElse(
          "SONATYPE_PASSWORD",
          {
            streams.value.log.warn("Undefined environment variable: SONATYPE_PASSWORD")
            "UNDEFINED"
          }
        )
      )
    ),
    licenses := Seq("MIT" -> url("https://opensource.org/licenses/MIT")),
    homepage := Some(url("https://github.com/mjakubowski84/parquet4s")),
    scmInfo := Some(
      ScmInfo(
        browseUrl = url("https://github.com/mjakubowski84/parquet4s"),
        connection = "scm:git@github.com:mjakubowski84/parquet4s.git"
      )
    ),
    sonatypeProjectHosting := Some(GitHubHosting(
      user = "mjakubowski84", repository = "parquet4s", email = "mjakubowski84@gmail.com")
    ),
    sonatypeProfileName := "com.github.mjakubowski84",
    developers := List(
      Developer(
        id = "mjakubowski84",
        name = "Marcin Jakubowski",
        email = "mjakubowski84@gmail.com",
        url = url("https://github.com/mjakubowski84")
      )
    ),
    publishMavenStyle := true,
    publishTo := Some(
      if (isSnapshot.value)
        Opts.resolver.mavenLocalFile
      else
        Opts.resolver.sonatypeStaging
    ),
    Test / publishArtifact := false,
    IntegrationTest / publishArtifact := false
  ) ++ (if (sys.env contains "SONATYPE_USER_NAME") Signing.signingSettings else Seq.empty)
}

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

lazy val core = (project in file("core"))
  .configs(IntegrationTest)
  .settings(
    name := "parquet4s-core",
    crossScalaVersions := supportedScalaVersions
  )
  .settings(itSettings)
  .settings(publishSettings)
  .settings(testReportSettings)

lazy val akka = (project in file("akka"))
  .configs(IntegrationTest)
  .settings(
    name := "parquet4s-akka",
    crossScalaVersions := supportedScalaVersions
  )
  .settings(itSettings)
  .settings(publishSettings)
  .settings(testReportSettings)
  .dependsOn(core % "compile->compile;it->it")

lazy val fs2 = (project in file("fs2"))
  .configs(IntegrationTest)
  .settings(
    name := "parquet4s-fs2",
    crossScalaVersions := fs2ScalaVersions
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
    publishLocal / skip := true
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
