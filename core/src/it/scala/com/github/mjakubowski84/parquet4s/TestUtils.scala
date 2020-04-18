package com.github.mjakubowski84.parquet4s

import java.io.File
import java.net.URI

import com.google.common.io.Files
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, FileUtil, Path}

trait TestUtils {

  protected val tempPath: Path = new Path(
    "file", "", Files.createTempDir().getAbsolutePath
  ).suffix("/testOutputPath")
  protected val tempPathString: String = tempPath.toString
  protected lazy val configuration = new Configuration()
  protected lazy val fileSystem: FileSystem = tempPath.getFileSystem(configuration)
  private val tempDir = new File(tempPath.toUri)

  def clearTemp(): Unit = FileUtil.fullyDelete(tempDir)

}
