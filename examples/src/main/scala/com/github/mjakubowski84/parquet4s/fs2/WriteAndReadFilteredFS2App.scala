package com.github.mjakubowski84.parquet4s.fs2

import cats.Show
import cats.effect.{IO, IOApp}
import com.github.mjakubowski84.parquet4s.{Col, Path}
import com.github.mjakubowski84.parquet4s.parquet._
import fs2.Stream
import fs2.io.file.Files

import scala.util.Random

object WriteAndReadFilteredFS2App extends IOApp.Simple {

  object Dict {
    val A = "A"
    val B = "B"
    val C = "C"
    val D = "D"

    val values: List[String] = List(A, B, C, D)
    def random: String = values(Random.nextInt(values.length))
  }

  case class Data(id: Int, dict: String)

  private implicit val showData: Show[Data] = Show.fromToString
  private val Count = 100

  override def run: IO[Unit] = {
    val stream = for {
      path <- Stream.resource(Files[IO].tempDirectory()).map(Path.apply)
      _ <- Stream.range[IO, Int](start = 0, stopExclusive = Count)
        .map { i => Data(id = i, dict = Dict.random) }
        .through(writeSingleFile(path.append("data.parquet")))
        .append(Stream.exec(IO.println("""dict == "A"""")))
        .append(fromParquet[IO, Data].filter(Col("dict") === Dict.A).read(path).printlns.drain)
        .append(Stream.exec(IO.println("""id >= 20 && id < 40""")))
        .append(fromParquet[IO, Data]
          .filter(Col("id") >= 20 && Col("id") < 40)
          .read(path)
          .printlns
          .drain
        )
    } yield ()

    stream.compile.drain
  }
}
