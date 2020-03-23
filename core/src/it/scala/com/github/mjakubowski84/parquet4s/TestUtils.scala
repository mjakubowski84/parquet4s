package com.github.mjakubowski84.parquet4s

import java.io.File
import java.net.URI

import com.google.common.io.Files
import org.apache.hadoop.fs.Path

trait TestUtils {

  protected val tempPath: Path = new Path(Files.createTempDir().getAbsolutePath, "testOutputPath")
  protected val tempPathString: String = tempPath.toString

  def clearTemp(): Unit = {
    val tempDir = new File(tempPath.makeQualified(URI.create("file:/"), new Path("/")).toUri)
    Option(tempDir.listFiles()).foreach(_.foreach(_.delete()))
    tempDir.delete()
  }

}
