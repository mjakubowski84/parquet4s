import sbt.Keys._
import sbt.{Credentials, Def, Developer, IntegrationTest, Opts, ScmInfo, Test, url}

object Releasing {

  lazy val publishSettings: Seq[Def.Setting[_]] =
    Seq(
      licenses := Seq("MIT" -> url("https://opensource.org/licenses/MIT")),
      homepage := Some(url("https://mjakubowski84.github.io/parquet4s/")),
      scmInfo := Some(
        ScmInfo(
          browseUrl  = url("https://github.com/mjakubowski84/parquet4s"),
          connection = "scm:git@github.com:mjakubowski84/parquet4s.git"
        )
      ),
      developers := List(
        Developer(
          id    = "mjakubowski84",
          name  = "Marcin Jakubowski",
          email = "mjakubowski84@gmail.com",
          url   = url("https://github.com/mjakubowski84")
        )
      ),
      publishMavenStyle := true,
      pomIncludeRepository := { _ => false },
      publishTo := {
        if (isSnapshot.value)
          Some(Opts.resolver.mavenLocalFile)
        else
          localStaging.value
      },
      Test / publishArtifact := false,
      IntegrationTest / publishArtifact := false
    ) ++ (if (sys.env contains "SONATYPE_USERNAME") Signing.signingSettings else Seq.empty)

}
