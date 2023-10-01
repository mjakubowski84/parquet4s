package com.github.mjakubowski84.parquet4s.scalapb

import com.github.mjakubowski84.parquet4s.ScalaPBImplicits.*
import com.github.mjakubowski84.parquet4s.protobuf.Data
import com.github.mjakubowski84.parquet4s.{ParquetReader, ParquetWriter, Path}

import java.nio.file.Files
import scala.util.Using

object WriteAndReadApp extends App {
  val data = (1 to 100).map(id => Data(id = id, text = id.toString))
  val path = Path(Files.createTempDirectory("example"))

  // write
  ParquetWriter.of[Data].writeAndClose(path.append("data.parquet"), data)

  // read
  Using.resource(ParquetReader.as[Data].read(path))(_.foreach(println))
}
