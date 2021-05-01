package com.github.mjakubowski84.parquet4s.fs2

import cats.Show
import cats.effect.{ExitCode, IO, IOApp}
import com.github.mjakubowski84.parquet4s.parquet._
import fs2.Stream
import fs2.io.file.Files

import scala.util.Random

object WriteAndReadFS2App extends IOApp.Simple {

  case class Data(id: Int, text: String)

  private implicit val showData: Show[Data] = Show.fromToString
  private val Count = 100

  override def run: IO[Unit] = {
    val stream = for {
      path <- Stream.resource(Files[IO].tempDirectory())
      _ <- Stream.range[IO, Int](start = 0, stopExclusive = Count)
        .map { i => Data(id = i, text = Random.nextString(4)) }
        .through(writeSingleFile(path.resolve("data.parquet").toString))
        .append(fromParquet[IO, Data].read(path.toString).printlns.drain)
    } yield ()

    stream.compile.drain.as(ExitCode.Success)
  }
}
