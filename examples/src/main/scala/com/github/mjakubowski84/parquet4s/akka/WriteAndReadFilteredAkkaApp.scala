package com.github.mjakubowski84.parquet4s.akka

import akka.actor.ActorSystem
import akka.stream.scaladsl.{Sink, Source}
import com.github.mjakubowski84.parquet4s.{Col, ParquetReader, ParquetStreams}
import com.google.common.io.Files

import scala.concurrent.Future
import scala.util.Random

object WriteAndReadFilteredAkkaApp extends App {

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
  val path = Files.createTempDir().getAbsolutePath

  implicit val system: ActorSystem = ActorSystem()
  import system.dispatcher

  val options = ParquetReader.Options()
  val printingSink = Sink.foreach(println)

  for {
    // write
    _ <- Source(data).runWith(ParquetStreams.toParquetSingleFile(s"$path/data.parquet"))
    // read filtered
    _ <- Future(println("""dict == "A""""))
    _ <- ParquetStreams.fromParquet[Data](path, options = options, filter = Col("dict") === Dict.A).runWith(printingSink)
    _ <- Future(println("""id >= 20 && id < 40"""))
    _ <- ParquetStreams.fromParquet[Data](path, options = options, filter = Col("id") >= 20 && Col("id") < 40).runWith(printingSink)
    // finish
    _ <- system.terminate()
  } yield ()

}
