package com.github.mjakubowski84.parquet4s.fs2

import cats.effect.{IO, IOApp}
import com.github.mjakubowski84.parquet4s.Path
import com.github.mjakubowski84.parquet4s.parquet.*
import fs2.io.file.Files
import fs2.Stream

import scala.util.Random
import org.apache.avro.SchemaBuilder
import org.apache.avro.generic.GenericRecordBuilder
import org.apache.parquet.avro.AvroParquetWriter
import org.apache.avro.generic.GenericRecord
import com.github.mjakubowski84.parquet4s.ParquetWriter
import com.github.mjakubowski84.parquet4s.ValueCodecConfiguration

object CustomAvroPartitioningWriteFS2App extends IOApp.Simple {
  private val Count = 100
  private val InputAvroSchema = SchemaBuilder
    .record("data")
    .namespace("example")
    .fields()
    .requiredInt("i")
    .requiredString("text")
    .requiredString("partition")
    .endRecord()
  private val PartitionedDataAvroSchema = SchemaBuilder
    .record("data")
    .namespace("example")
    .fields()
    .requiredInt("i")
    .requiredString("text")
    .endRecord()

  val data = (1 to Count).map { i =>
    new GenericRecordBuilder(InputAvroSchema)
      .set("i", i)
      .set("text", Random.nextString(4))
      .set("partition", (i % 4).toString())
      .build()
  }

  val vcc = ValueCodecConfiguration.Default

  def write(basePath: Path) =
    viaParquet[IO]
      .custom[GenericRecord, AvroParquetWriter.Builder[GenericRecord]](path =>
        AvroParquetWriter
          .builder[GenericRecord](path.toOutputFile(ParquetWriter.Options()))
          .withSchema(PartitionedDataAvroSchema)
      )
      .partitionUsing { case (path, record) =>
        val partitionValue = record.get("partition")
        val partitionedRecord = new GenericRecordBuilder(PartitionedDataAvroSchema)
          .set("i", record.get("i"))
          .set("text", record.get("text"))
          .build()
        (path.append(s"partition=$partitionValue"), partitionedRecord)
      }
      .write(basePath)

  def read(basePath: Path) =
    fromParquet[IO].generic
      .read(basePath)

  override def run: IO[Unit] = {
    val stream = for {
      path <- Stream
        .resource(Files[IO].tempDirectory(None, "", None))
        .map(fs2Path => Path(fs2Path.toNioPath).append("data.parquet"))
      _ <- Stream
        .iterable(data)
        .through(write(path))
        .append(
          read(path).evalTap(r =>
            IO.println(
              s"i=${r.get[Int]("i", vcc)}, text=${r.get[String]("text", vcc)}, partition=${r.get[String]("partition", vcc)}"
            )
          )
        )
    } yield ()

    stream.compile.drain
  }
}
