package com.github.mjakubowski84.parquet4s.scalapb

import com.github.mjakubowski84.parquet4s.ScalaPBImplicits.*
import com.github.mjakubowski84.parquet4s.protobuf.Data
import com.github.mjakubowski84.parquet4s.{ParquetReader, ParquetWriter, Path}

import java.nio.file.Files

object WriteIncrementallyAndReadApp extends App {
  val count = 100
  val data  = (1 to count).map(id => Data(id = id, text = id.toString))
  val path  = Path(Files.createTempDirectory("example"))

  // write
  val writer = ParquetWriter.of[Data].build(path.append("data.parquet"))
  try data.foreach(entity => writer.write(entity))
  finally writer.close()

  // read
  val readData = ParquetReader.as[Data].read(path)
  try readData.foreach(println)
  finally readData.close()
}
