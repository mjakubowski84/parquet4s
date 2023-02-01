package com.github.mjakubowski84.parquet4s.fs2

import cats.effect.{IO, IOApp}
import com.github.mjakubowski84.parquet4s.Path
import com.github.mjakubowski84.parquet4s.parquet.*
import com.github.mjakubowski84.parquet4s.protobuf.DataOuterClass.Data
import fs2.io.file.Files
import fs2.{Pipe, Stream}
import org.apache.parquet.proto.ProtoParquetWriter

import scala.util.Random

object CustomParquetWriterFS2App extends IOApp.Simple {
  private val Count = 100

  override def run: IO[Unit] = {
    def write(path: Path): Pipe[IO, Data, Nothing] = {
      val builder = ProtoParquetWriter.builder[Data](path.hadoopPath).withMessage(classOf[Data])
      writeSingleFile[IO]
        .custom[Data, ProtoParquetWriter.Builder[Data]](builder)
        .write
    }

    val stream = for {
      path <- Stream
        .resource(Files[IO].tempDirectory(None, "", None))
        .map(fs2Path => Path(fs2Path.toNioPath).append("data.parquet"))
      _ <- Stream
        .range[IO, Int](start = 0, stopExclusive = Count)
        .map(i => Data.newBuilder.setId(i).setText(Random.nextString(4)).build)
        .through(write(path))
    } yield ()

    stream.compile.drain
  }
}
