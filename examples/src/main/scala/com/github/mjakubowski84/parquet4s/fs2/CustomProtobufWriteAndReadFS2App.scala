package com.github.mjakubowski84.parquet4s.fs2

import cats.effect.{IO, IOApp}
import com.github.mjakubowski84.parquet4s.Path
import com.github.mjakubowski84.parquet4s.parquet.*
import com.github.mjakubowski84.parquet4s.protobuf.DataOuterClass.Data
import fs2.io.file.Files
import fs2.{Pipe, Stream}
import org.apache.parquet.proto._

import scala.util.Random
import com.google.protobuf.TextFormat

/** Please note! This is an example of Java Protobuf + Parquet4s using custom readers and writers. You can also use
  * Scala Protobuf with regular Parquet4s functions thanks to ScalaPB module of Parquet4s.
  */
object CustomProtobufWriteAndReadFS2App extends IOApp.Simple {
  private val Count = 100

  def write(path: Path): Pipe[IO, Data, Nothing] = {
    val builder = ProtoParquetWriter.builder[Data](path.hadoopPath).withMessage(classOf[Data])
    writeSingleFile[IO]
      .custom[Data, ProtoParquetWriter.Builder[Data]](builder)
      .write
  }

  def read(path: Path) =
    fromParquet[IO]
      .custom(ProtoParquetReader.builder[Data.Builder](path.hadoopPath))
      .read(_.build)

  override def run: IO[Unit] = {

    val stream = for {
      path <- Stream
        .resource(Files[IO].tempDirectory(None, "", None))
        .map(fs2Path => Path(fs2Path.toNioPath).append("data.parquet"))
      _ <- Stream
        .range[IO, Int](start = 0, stopExclusive = Count)
        .map(i => Data.newBuilder.setId(i).setText(Random.nextString(4)).build)
        .through(write(path))
        .append(
          read(path).evalMapChunk(data => IO.println(TextFormat.printer().escapingNonAscii(false).printToString(data)))
        )
    } yield ()

    stream.compile.drain
  }
}
