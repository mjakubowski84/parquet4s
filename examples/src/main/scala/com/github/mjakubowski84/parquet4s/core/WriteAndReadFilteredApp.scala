package com.github.mjakubowski84.parquet4s.core

import com.github.mjakubowski84.parquet4s.{Col, ParquetReader, ParquetWriter, Path}

import java.nio.file.Files
import scala.util.Random

object WriteAndReadFilteredApp extends App {

  object Dict {
    val A = "A"
    val B = "B"
    val C = "C"
    val D = "D"

    val values: List[String] = List(A, B, C, D)
    def random: String = values(Random.nextInt(values.length))
  }

  case class Data(id: Int, dict: String)

  val count = 100
  val data = (1 to count).map { i => Data(id = i, dict = Dict.random) }
  val path = Path(Files.createTempDirectory("example"))

  // write
  ParquetWriter.of[Data].writeAndClose(path.append("data.parquet"), data)

  //read filtered
  println("""dict == "A"""")
  val dictIsOnlyA = ParquetReader.as[Data].filter(Col("dict") === Dict.A).read(path)
  try {
    dictIsOnlyA.foreach(println)
  } finally dictIsOnlyA.close()

  println("""id >= 20 && id < 40""")
  val idIsBetween10And90 = ParquetReader.as[Data].filter(Col("id") >= 20 && Col("id") < 40).read(path)
  try {
    idIsBetween10And90.foreach(println)
  } finally idIsBetween10And90.close()

}
