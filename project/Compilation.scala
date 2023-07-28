import sbt.Keys._
import sbt.CrossVersion

object Compilation {

  lazy val compilationSettings = Seq(
    scalacOptions ++= {
      Seq(
        "-encoding",
        "UTF-8",
        "-release:8",
        "-feature",
        "-language:implicitConversions",
        "-language:higherKinds",
        "-Xfatal-warnings"
      ) ++ {
        CrossVersion.partialVersion(scalaVersion.value) match {
          case Some((3, _)) =>
            Seq(
              "-unchecked",
              "-explain-types",
              "-Wunused:implicits", // Warn if an implicit parameter is unused.
              "-Wunused:explicits", // Warn if an explicit parameter is unused.
              "-Wunused:imports", // Warn if an import selector is not referenced.
              "-Wunused:locals", // Warn if a local definition is unused.
              "-Wunused:params", // Warn if a value parameter is unused.
              "-Wunused:privates" // Warn if a private member is unused.
            )
          case Some((2, 13)) =>
            Seq(
              "-deprecation",
              "-Xsource:3",
              "-explaintypes",
              "-Wextra-implicit", // Warn when more than one implicit parameter section is defined.
              "-Wnumeric-widen", // Warn when numerics are widened.
              "-Wunused:implicits", // Warn if an implicit parameter is unused.
              "-Wunused:explicits", // Warn if an explicit parameter is unused.
              "-Wunused:imports", // Warn if an import selector is not referenced.
              "-Wunused:locals", // Warn if a local definition is unused.
              "-Wunused:params", // Warn if a value parameter is unused.
              "-Wunused:patvars", // Warn if a variable bound in a pattern is unused.
              "-Wunused:privates" // Warn if a private member is unused.
            )
          case _ =>
            Seq(
              "-deprecation",
              "-Xsource:3",
              "-explaintypes",
              "-Ywarn-extra-implicit", // Warn when more than one implicit parameter section is defined.
              "-Ywarn-inaccessible", // Warn about inaccessible types in method signatures.
              "-Ywarn-numeric-widen", // Warn when numerics are widened.
              "-Ywarn-unused:implicits", // Warn if an implicit parameter is unused.
              "-Ywarn-unused:imports", // Warn if an import selector is not referenced.
              "-Ywarn-unused:locals", // Warn if a local definition is unused.
              "-Ywarn-unused:params", // Warn if a value parameter is unused.
              "-Ywarn-unused:patvars", // Warn if a variable bound in a pattern is unused.
              "-Ywarn-unused:privates" // Warn if a private member is unused.
            )
        }
      }
    }
  )

}
