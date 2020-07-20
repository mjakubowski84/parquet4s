package com.github.mjakubowski84.parquet4s.fs2

import java.nio.file.Paths

import cats.Show
import cats.effect.{Blocker, ExitCode, IO, IOApp}
import com.github.mjakubowski84.parquet4s.Col
import com.github.mjakubowski84.parquet4s.parquet._
import fs2.Stream
import fs2.io.file.tempDirectoryStream

import scala.util.Random

object WriteAndReadFilteredFS2App extends IOApp {

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
  private val TmpPath = Paths.get(sys.props("java.io.tmpdir"))

  override def run(args: List[String]): IO[ExitCode] = {
    val stream = for {
      blocker <- Stream.resource(Blocker[IO])
      path <- tempDirectoryStream[IO](blocker, dir = TmpPath)
      _ <- Stream.range[IO](start = 0, stopExclusive = Count)
        .map { i => Data(id = i, dict = Dict.random) }
        .through(writeSingleFile(blocker, path.resolve("data.parquet").toString))
        .append(Stream.eval_(IO(println("""dict == "A""""))))
        .append(read[Data, IO](blocker, path.toString, filter = Col("dict") === Dict.A).showLinesStdOut.drain)
        .append(Stream.eval_(IO(println("""id >= 20 && id < 40"""))))
        .append(read[Data, IO](blocker, path.toString, filter = Col("id") >= 20 && Col("id") < 40).showLinesStdOut.drain)
    } yield ()

    stream.compile.drain.as(ExitCode.Success)
  }
}
