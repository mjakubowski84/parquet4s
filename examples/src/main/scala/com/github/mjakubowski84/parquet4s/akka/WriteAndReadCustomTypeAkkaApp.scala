package com.github.mjakubowski84.parquet4s.akka

import akka.actor.ActorSystem
import akka.stream.scaladsl.{Sink, Source}
import com.github.mjakubowski84.parquet4s.CustomType._
import com.github.mjakubowski84.parquet4s.{ParquetStreams, Path}

import java.nio.file.Files

object WriteAndReadCustomTypeAkkaApp extends App {

  object Data {
    def generate(count: Int): Iterator[Data] = Iterator.range(1, count).map(i => Data(id = i, dict = Dict.random))
  }
  case class Data(id: Long, dict: Dict.Type)

  val data = () => Data.generate(count = 100)
  val path = Path(Files.createTempDirectory("example"))

  implicit val system: ActorSystem = ActorSystem()
  import system.dispatcher

  for {
    // write
    _ <- Source.fromIterator(data).runWith(ParquetStreams.toParquetSingleFile.of[Data].build(path.append("data.parquet")))
    // read
    // hint: you can filter by dict using string value, for example: filter = Col("dict") === "A"
    _ <- ParquetStreams.fromParquet.as[Data].read(path).runWith(Sink.foreach(println))
    // finish
    _ <- system.terminate()
  } yield ()

}
