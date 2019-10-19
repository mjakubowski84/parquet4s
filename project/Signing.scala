import java.io.File

import sbt.Keys._
import sbt.io.IO
import sbt.librarymanagement.{Artifact, Configuration}
import sbt.{Def, TaskKey, _}

import scala.language.postfixOps

object Signing {

  private val SignatureExtension = "asc"

  private def createSignatureFile(artifactFile: File): File = {
    val signatureFile = file(artifactFile.getAbsolutePath + "." + SignatureExtension)
    if (signatureFile.exists()) IO.delete(signatureFile)

    val command = "gpg"
    val keyRingArgs = Seq("--no-default-keyring", "--keyring", (file(System.getProperty("user.home")) / ".gnupg" / "pubring.kbx").getAbsolutePath)
    val actionArgs = Seq("--detach-sign", "--armor")
    val passwordArgs = Seq("--pinentry-mode", "loopback", "--passphrase", sys.env("GPG_PASSWORD"))
    val outputArgs = Seq("--output", signatureFile.getAbsolutePath)
    val targetArgs = Seq(artifactFile.getAbsolutePath)
    val args: Seq[String] = keyRingArgs ++ actionArgs ++ passwordArgs ++ outputArgs ++ targetArgs

    sys.process.Process(command, args) ! match {
      case 0 => ()
      case n => sys.error(s"""Failure running '${args.mkString(command + " ", " ", "")}'. Exit code: $n""")
    }

    signatureFile
  }

  private def addSignatureArtifact(configuration: Configuration, packageTask: TaskKey[File]): Def.SettingsDefinition = {

    val signTaskDef = Def.task {
      val (artifact, artifactFile) = packagedArtifact.in(configuration, packageTask).value
      streams.value.log.info("Signing: " + artifact)
      createSignatureFile(artifactFile)
    }

    val artifactDef = Def.setting {
      val sourceArtifact = artifact.in(configuration, packageTask).value
      Artifact(
        name = sourceArtifact.name,
        `type` = SignatureExtension,
        extension = sourceArtifact.extension + "." + SignatureExtension,
        classifier = sourceArtifact.classifier,
        configurations = Vector.empty,
        url = None
      )
    }

    addArtifact(artifactDef, signTaskDef)
  }

  def signingSettings: Seq[Def.Setting[_]] =
    addSignatureArtifact(Compile, packageBin).settings ++
    addSignatureArtifact(Compile, packageDoc).settings ++
    addSignatureArtifact(Compile, packageSrc).settings ++
    addSignatureArtifact(Compile, makePom).settings

}
