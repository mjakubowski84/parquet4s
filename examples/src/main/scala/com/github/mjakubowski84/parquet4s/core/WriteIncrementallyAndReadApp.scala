package com.github.mjakubowski84.parquet4s.core

import com.github.mjakubowski84.parquet4s.{ParquetReader, ParquetWriter, Path}

import java.nio.file.Files
import scala.util.Random
import scala.util.Using

object WriteIncrementallyAndReadApp extends App {

  case class Data(id: Int, text: String)

  val count = 100
  val data  = (1 to count).map(i => Data(id = i, text = Random.nextString(4)))
  val path  = Path(Files.createTempDirectory("example"))

  // write
  val writer = ParquetWriter.of[Data].build(path.append("data.parquet"))
  try data.foreach(entity => writer.write(entity))
  finally writer.close()

  // read
  Using.resource(ParquetReader.as[Data].read(path))(_.foreach(println))

}
