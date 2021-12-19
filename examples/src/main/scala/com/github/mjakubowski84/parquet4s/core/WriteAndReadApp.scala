package com.github.mjakubowski84.parquet4s.core

import com.github.mjakubowski84.parquet4s.{ParquetReader, ParquetWriter, Path}

import java.nio.file.Files
import scala.util.Random

object WriteAndReadApp extends App {

  case class Data(id: Int, text: String)

  val count = 100
  val data  = (1 to count).map(i => Data(id = i, text = Random.nextString(4)))
  val path  = Path(Files.createTempDirectory("example"))

  // write
  ParquetWriter.of[Data].writeAndClose(path.append("data.parquet"), data)

  // read
  val readData = ParquetReader.as[Data].read(path)
  try readData.foreach(println)
  finally readData.close()

}
