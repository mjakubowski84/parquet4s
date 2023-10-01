package com.github.mjakubowski84.parquet4s.core

import com.github.mjakubowski84.parquet4s.{ParquetReader, ParquetWriter, Path}

import java.nio.file.Files
import scala.util.Random
import scala.util.Using
import com.github.mjakubowski84.parquet4s.RecordFilter

object WriteAndReadUsingRecordFilterApp extends App {

  case class Data(id: Int, text: String)

  val count = 100
  val data  = (1 to count).map(i => Data(id = i, text = Random.nextString(4)))
  val path  = Path(Files.createTempDirectory("example"))

  // write
  ParquetWriter.of[Data].writeAndClose(path.append("data.parquet"), data)

  // skips all but last 3 records (out of 100)
  Using.resource(ParquetReader.as[Data].filter(RecordFilter(_ >= 97)).read(path))(_.foreach(println))
}
