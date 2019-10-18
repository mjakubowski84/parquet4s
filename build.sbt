import bloop.integrations.sbt.BloopDefaults

lazy val resolvers =  Seq(
  Opts.resolver.sonatypeReleases,
  Resolver.jcenterRepo
)

lazy val supportedScalaVersions = Seq("2.11.12", "2.12.10")

lazy val commonSettings = Seq(
  Keys.organization := "com.github.mjakubowski84",
  Keys.version := "0.12.0-SNAPSHOT",
  Keys.isSnapshot := true,
  Keys.scalaVersion := "2.11.12",
  Keys.scalacOptions ++= Seq("-deprecation", "-target:jvm-1.8"),
  Keys.javacOptions ++= Seq("-source", "1.8", "-target", "1.8", "-unchecked", "-deprecation", "-feature"),
  Keys.resolvers := resolvers,
  Keys.makePomConfiguration := makePomConfiguration.value.withConfigurations(Configurations.defaultMavenConfigurations)
)

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
    Keys.licenses := Seq("MIT" -> url("https://opensource.org/licenses/MIT")),
    Keys.homepage := Some(url("https://github.com/mjakubowski84/parquet4s")),
    Keys.scmInfo := Some(
      ScmInfo(
        browseUrl = url("https://github.com/mjakubowski84/parquet4s"),
        connection = "scm:git@github.com:mjakubowski84/parquet4s.git"
      )
    ),
    SonatypeKeys.sonatypeProjectHosting := Some(GitHubHosting(
      user = "mjakubowski84", repository = "parquet4s", email = "mjakubowski84@gmail.com")
    ),
    SonatypeKeys.sonatypeProfileName := "com.github.mjakubowski84",
    Keys.developers := List(
      Developer(
        id = "mjakubowski84",
        name = "Marcin Jakubowski",
        email = "mjakubowski84@gmail.com",
        url = url("https://github.com/mjakubowski84")
      )
    ),
    Keys.publishMavenStyle := true,
    Keys.publishTo := Some(
      if (isSnapshot.value)
        Opts.resolver.mavenLocalFile
      else
        Opts.resolver.sonatypeStaging
    ),
    Keys.publishArtifact in Test := false,
    Keys.publishArtifact in IntegrationTest := false
  ) ++ Signing.signingSettings
}

lazy val itSettings = Defaults.itSettings ++ Project.inConfig(IntegrationTest)(Seq(
  Keys.fork := true,
  Keys.parallelExecution := true  
)) ++ Project.inConfig(IntegrationTest)(BloopDefaults.configSettings)

lazy val core = (project in file("core"))
  .configs(IntegrationTest)
  .settings(
    Keys.name := "parquet4s-core",
    Keys.crossScalaVersions := supportedScalaVersions
  )
  .settings(commonSettings)
  .settings(itSettings)
  .settings(publishSettings)

lazy val akka = (project in file("akka"))
  .configs(IntegrationTest)
  .settings(
    Keys.name := "parquet4s-akka",
    Keys.crossScalaVersions := supportedScalaVersions
  )
  .settings(commonSettings)
  .settings(itSettings)
  .settings(publishSettings)
  .dependsOn(core % "compile->compile;it->it")

lazy val examples = (project in file("examples"))
  .settings(commonSettings)
  .settings(
    Keys.name := "parquet4s-examples",
    publish / skip := true,
    publishLocal / skip := true
  )
  .dependsOn(akka)

lazy val root = (project in file("."))
  .settings(commonSettings)
  .settings(publishSettings)
  .settings(
    crossScalaVersions := Nil,
    publish / skip := true,
    publishLocal / skip := true
  )
  .aggregate(core, akka, examples)
