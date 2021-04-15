package com.github.mjakubowski84.parquet4s

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, FileUtil, Path}

import java.io.File
import java.nio.file.Files

trait TestUtils {

  protected val tempPath: Path = new Path(
    "file", "", Files.createTempDirectory("example").toString
  ).suffix("/testOutputPath")
  protected val tempPathString: String = tempPath.toString
  protected lazy val configuration = new Configuration()
  protected lazy val fileSystem: FileSystem = tempPath.getFileSystem(configuration)
  private val tempDir = new File(tempPath.toUri)

  def clearTemp(): Unit = FileUtil.fullyDelete(tempDir)

}
