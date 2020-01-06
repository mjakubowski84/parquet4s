package com.github.mjakubowski84.parquet4s.akka

import akka.actor.ActorSystem
import akka.stream.scaladsl.{Sink, Source}
import akka.stream.{ActorMaterializer, Materializer}
import com.github.mjakubowski84.parquet4s.CustomType._
import com.github.mjakubowski84.parquet4s.ParquetStreams
import com.google.common.io.Files

object WriteAndReadCustomTypeAkkaApp extends App {

  object Data {
    def generate(count: Int): Seq[Data] = (1 to count).map { i => Data(id = i, dict = Dict.random) }
  }
  case class Data(id: Long, dict: Dict.Type)

  val data = Data.generate(count = 100)
  val path = Files.createTempDir().getAbsolutePath

  implicit val system: ActorSystem = ActorSystem()
  implicit val materializer: Materializer = ActorMaterializer()
  import system.dispatcher

  for {
    // write
    _ <- Source(data).runWith(ParquetStreams.toParquetSingleFile(s"$path/data.parquet"))
    // read
    // hint: you can filter by dict using string value, for example: filter = Col("dict") === "A"
    _ <- ParquetStreams.fromParquet[Data](path).runWith(Sink.foreach(println))
    // finish
    _ <- system.terminate()
  } yield ()

}
