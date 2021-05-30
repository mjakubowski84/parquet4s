package com.github.mjakubowski84.parquet4s

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, FileUtil}

import java.io.File
import java.nio.file.Files

trait TestUtils {

  protected val tempPath: Path = Path(Path(Files.createTempDirectory("example")), "testOutputPath")
  protected lazy val configuration = new Configuration()
  protected lazy val fileSystem: FileSystem = tempPath.toHadoop.getFileSystem(configuration)
  private val tempDir = new File(tempPath.toUri)

  def clearTemp(): Unit = {
    FileUtil.fullyDelete(tempDir)
    ()
  }

}
