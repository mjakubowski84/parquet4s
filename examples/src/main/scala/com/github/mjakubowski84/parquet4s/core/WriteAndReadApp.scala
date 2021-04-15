package com.github.mjakubowski84.parquet4s.core

import com.github.mjakubowski84.parquet4s.{ParquetReader, ParquetWriter}

import java.nio.file.Files
import scala.util.Random

object WriteAndReadApp extends App {

  case class Data(id: Int, text: String)

  val count = 100
  val data = (1 to count).map { i => Data(id = i, text = Random.nextString(4)) }
  val path = Files.createTempDirectory("example").toString

  // write
  ParquetWriter.writeAndClose(s"$path/data.parquet", data)

  //read
  val readData = ParquetReader.read[Data](path)
  try {
    readData.foreach(println)
  } finally readData.close()

}
