package com.github.mjakubowski84.parquet4s.akka

import akka.actor.ActorSystem
import akka.stream.scaladsl.{Sink, Source}
import com.github.mjakubowski84.parquet4s.ParquetStreams

import java.nio.file.Files
import scala.util.Random

object WriteAndReadAkkaApp extends App {

  case class Data(id: Int, text: String)

  val count = 100
  val data = (1 to count).map { i => Data(id = i, text = Random.nextString(4)) }
  val path = Files.createTempDirectory("example").toString

  implicit val system: ActorSystem = ActorSystem()
  import system.dispatcher

  for {
    // write
    _ <- Source(data).runWith(ParquetStreams.toParquetSingleFile(s"$path/data.parquet"))
    // read
    _ <- ParquetStreams.fromParquet[Data].read(path).runWith(Sink.foreach(println))
    // finish
    _ <- system.terminate()
  } yield ()

}
