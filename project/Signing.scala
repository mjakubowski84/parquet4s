import sbt.Keys._
import sbt.internal.util.ManagedLogger
import sbt.io.IO
import sbt.librarymanagement.{Artifact, Configuration}
import sbt._

import java.io.File
import scala.language.postfixOps
import scala.sys.process.ProcessLogger

object Signing {

  private val SignatureExtension = "asc"
  private val VersionLine = """gpg \(GnuPG\) ([0-9.]+)""".r
  private val GPG1 = 1

  private def createSignatureFile(artifactFile: File, logger: ManagedLogger): File = {
    val signatureFile = file(artifactFile.getAbsolutePath + "." + SignatureExtension)
    if (signatureFile.exists()) IO.delete(signatureFile)

    val command = "gpg"
//    val keyRingArgs = Seq("--no-default-keyring", "--keyring", (file(System.getProperty("user.home")) / ".gnupg" / "pubring.kbx").getAbsolutePath)
    val actionArgs = Seq("--detach-sign", "--armor")
    val passwordArgs = if (gpgVersion == GPG1) {
      Seq("--batch", "--passphrase", sys.env("GPG_PASSWORD"))
    } else {
      Seq("--pinentry-mode", "loopback", "--passphrase", sys.env("GPG_PASSWORD"))
    }
    val outputArgs = Seq("--output", signatureFile.getAbsolutePath)
    val targetArgs = Seq(artifactFile.getAbsolutePath)
    val args: Seq[String] = actionArgs ++ passwordArgs ++ outputArgs ++ targetArgs

    sys.process.Process(command, args) ! ProcessLogger(
      fout = out => logger.info(out),
      ferr = err => logger.error(err)
    ) match {
      case 0 => ()
      case _ => sys.error(s"""Failure running '${args.mkString(command + " ", " ", "")}'.""")
    }

    signatureFile
  }

  private def gpgVersion: Int =
    // gpg (GnuPG) 1.4.23
    // gpg (GnuPG) 2.2.27
    sys.process.Process("gpg", Seq("--version")).lineStream.collectFirst {
      case VersionLine(versionString) => versionString.substring(0, 1).toInt
    } match {
      case Some(version) => version
      case None => sys.error("Failed to resolve GPG version")
    }

  private def addSignatureArtifact(configuration: Configuration, packageTask: TaskKey[File]): Def.SettingsDefinition = {

    val signTaskDef = Def.task {
      val (artifact, artifactFile) = packagedArtifact.in(configuration, packageTask).value
      streams.value.log.info("Signing: " + artifact)
      createSignatureFile(artifactFile, streams.value.log)
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
