package com.github.mjakubowski84.parquet4s.fs2

import java.nio.file.Paths

import cats.effect.{Blocker, ExitCode, IO, IOApp}
import com.github.mjakubowski84.parquet4s.parquet._
import fs2.Stream
import fs2.io.file.tempDirectoryStream
import cats.{Show, implicits}

import scala.util.Random

object WriteAndReadFS2App extends IOApp {

  case class Data(id: Int, text: String)

  private implicit val showData: Show[Data] = Show.fromToString
  private val Count = 100
  private val TmpPath = Paths.get(sys.props("java.io.tmpdir"))

  override def run(args: List[String]): IO[ExitCode] = {
    val stream = for {
      blocker <- Stream.resource(Blocker[IO])
      path <- tempDirectoryStream[IO](blocker, dir = TmpPath)
      _ <- Stream.range[IO](start = 0, stopExclusive = Count)
        .map { i => Data(id = i, text = Random.nextString(4)) }
        .through(writeSingleFile(blocker, path.resolve("data.parquet").toString))
        .append(fromParquet[IO, Data].read(blocker, path.toString).showLinesStdOut.drain)
    } yield ()

    stream.compile.drain.as(ExitCode.Success)
  }
}
