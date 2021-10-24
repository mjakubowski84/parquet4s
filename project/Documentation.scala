import com.typesafe.sbt.site.SitePlugin.autoImport.makeSite
import microsites.MicrositeKeys._
import sbt.Keys._
import sbt.{Def, url}
import sbt.io.FileFilter._
import sbt.io.syntax._

object Documentation {

  lazy val documentationSettings: Seq[Def.Setting[_]] =
    Seq(
      name := "Parquet4s",
      description := "Read and write Parquet files using Scala",
      organizationName := "Marcin Jakubowski",
      organizationHomepage := Some(url("https://github.com/mjakubowski84")),
      micrositeDocumentationUrl := "docs",
      micrositeFooterText := None,
      micrositeBaseUrl := "parquet4s",
      micrositeGitterChannel := false,
      micrositeGithubOwner := "mjakubowski84",
      micrositeGithubRepo := "parquet4s",
      micrositeGithubToken := sys.env.get("PARQUET4S_DOCS_GITHUB_TOKEN"),
      micrositePushSiteWith := GitHub4s,
      makeSite / includeFilter := "*.html" || "*.css" || "*.png" || "*.jpg" || "*.gif" || "*.js" || "*.md" || "*.svg",
      micrositeDataDirectory := baseDirectory.value / "docs" / "data"
    )

}
