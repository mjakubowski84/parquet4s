package com.github.mjakubowski84.parquet4s.akkaPekko

import com.github.mjakubowski84.parquet4s.ScalaCompat.actor.ActorSystem
import com.github.mjakubowski84.parquet4s.ScalaCompat.stream.scaladsl.Source
import com.github.mjakubowski84.parquet4s.{ParquetStreams, Path}
import org.apache.parquet.avro.AvroParquetWriter

import java.nio.file.Files
import scala.util.Random
import org.apache.avro.generic.GenericRecordBuilder
import com.github.mjakubowski84.parquet4s.ParquetWriter
import org.apache.parquet.avro.AvroParquetReader
import com.github.mjakubowski84.parquet4s.ParquetReader
import org.apache.avro.SchemaBuilder
import org.apache.avro.generic.GenericRecord

object CustomAvroWriteAndReadAkkaPekkoApp extends App {
  val avroSchema = SchemaBuilder
    .record("data")
    .namespace("example")
    .fields()
    .requiredInt("i")
    .requiredString("text")
    .endRecord()
  val count = 100
  val data = (1 to count).map { i =>
    new GenericRecordBuilder(avroSchema)
      .set("i", i)
      .set("text", Random.nextString(4))
      .build()
  }
  val path = Path(Files.createTempDirectory("example")).append("data.parquet")

  implicit val system: ActorSystem = ActorSystem()
  import system.dispatcher

  lazy val writerBuilder = AvroParquetWriter
    .builder[GenericRecord](path.toOutputFile(ParquetWriter.Options()))
    .withSchema(avroSchema)

  lazy val writerSink = ParquetStreams.toParquetSingleFile
    .custom[GenericRecord, AvroParquetWriter.Builder[GenericRecord]](writerBuilder)
    .write

  lazy val readerBuilder = AvroParquetReader
    .builder[GenericRecord](path.toInputFile(ParquetReader.Options()))

  lazy val readerSource = ParquetStreams.fromParquet
    .custom[GenericRecord](readerBuilder)
    .read()

  val stream = for {
    _ <- Source(data).runWith(writerSink)
    _ <- readerSource.runForeach(println)
  } yield ()

  stream.andThen { case _ =>
    system.terminate()
  }
}
