package com.github.mjakubowski84.parquet4s.core

import com.github.mjakubowski84.parquet4s.CustomType._
import com.github.mjakubowski84.parquet4s.ParquetSchemaResolver._
import com.github.mjakubowski84.parquet4s.{ParquetReader, ParquetWriter, Path}

import java.nio.file.Files

object WriteAndReadCustomTypeApp extends App {

  object Data {
    def generate(count: Int): Iterable[Data] = (1 to count).map(i => Data(id = i, dict = Dict.random))
  }
  case class Data(id: Long, dict: Dict.Type)

  val data = Data.generate(count = 100)
  val path = Path(Files.createTempDirectory("example"))

  // write
  ParquetWriter.writeAndClose(path.append("data.parquet"), data)

  //read
  val readData = ParquetReader.read[Data](path)
  // hint: you can filter by dict using string value, for example: filter = Col("dict") === "A"
  try readData.foreach(println)
  finally readData.close()

}
