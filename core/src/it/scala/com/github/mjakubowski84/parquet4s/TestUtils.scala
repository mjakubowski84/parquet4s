package com.github.mjakubowski84.parquet4s

import java.nio.file.{Path, Paths}

import com.google.common.io.Files

trait TestUtils {

  private val tempPath: Path = Paths.get(Files.createTempDir().getAbsolutePath, "testOutputPath")

  val tempPathString: String = tempPath.toString

  def clearTemp(): Unit = {
    val tempDir = tempPath.toFile
    Option(tempDir.listFiles()).foreach(_.foreach(_.delete()))
    tempDir.delete()
  }

}
