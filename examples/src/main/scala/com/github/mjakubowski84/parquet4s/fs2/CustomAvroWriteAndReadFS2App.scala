package com.github.mjakubowski84.parquet4s.fs2

import cats.effect.{IO, IOApp}
import com.github.mjakubowski84.parquet4s.Path
import com.github.mjakubowski84.parquet4s.parquet.*
import fs2.io.file.Files
import fs2.{Pipe, Stream}

import scala.util.Random
import org.apache.avro.SchemaBuilder
import org.apache.avro.generic.GenericRecordBuilder
import org.apache.parquet.avro.AvroParquetWriter
import org.apache.avro.generic.GenericRecord
import com.github.mjakubowski84.parquet4s.ParquetWriter
import com.github.mjakubowski84.parquet4s.ParquetReader
import org.apache.parquet.avro.AvroParquetReader

object CustomAvroWriteAndReadFS2App extends IOApp.Simple {
  val Count = 100
  val AvroSchema = SchemaBuilder
    .record("data")
    .namespace("example")
    .fields()
    .requiredInt("i")
    .requiredString("text")
    .endRecord()

  val data = (1 to Count).map { i =>
    new GenericRecordBuilder(AvroSchema)
      .set("i", i)
      .set("text", Random.nextString(4))
      .build()
  }

  def write(path: Path): Pipe[IO, GenericRecord, Nothing] = {
    val builder =
      AvroParquetWriter.builder[GenericRecord](path.toOutputFile(ParquetWriter.Options())).withSchema(AvroSchema)

    writeSingleFile[IO].custom[GenericRecord, AvroParquetWriter.Builder[GenericRecord]](builder).write
  }

  def read(path: Path) =
    fromParquet[IO]
      .custom(AvroParquetReader.builder[GenericRecord](path.toInputFile(ParquetReader.Options())))
      .read()

  override def run: IO[Unit] = {
    val stream = for {
      path <- Stream
        .resource(Files[IO].tempDirectory(None, "", None))
        .map(fs2Path => Path(fs2Path.toNioPath).append("data.parquet"))
      _ <- Stream
        .iterable(data)
        .through(write(path))
        .append(read(path).printlns)
    } yield ()

    stream.compile.drain
  }
}
