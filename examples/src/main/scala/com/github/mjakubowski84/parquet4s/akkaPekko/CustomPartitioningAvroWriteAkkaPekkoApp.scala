package com.github.mjakubowski84.parquet4s.akkaPekko

import com.github.mjakubowski84.parquet4s.ScalaCompat.actor.ActorSystem
import com.github.mjakubowski84.parquet4s.ScalaCompat.stream.scaladsl.Sink
import com.github.mjakubowski84.parquet4s.ScalaCompat.stream.scaladsl.Source
import com.github.mjakubowski84.parquet4s.{ParquetStreams, Path}
import org.apache.parquet.avro.AvroParquetWriter

import java.nio.file.Files
import scala.util.Random
import org.apache.avro.generic.GenericRecordBuilder
import com.github.mjakubowski84.parquet4s.ParquetWriter
import org.apache.avro.SchemaBuilder
import org.apache.avro.generic.GenericRecord
import com.github.mjakubowski84.parquet4s.ValueCodecConfiguration

object CustomPartitioningAvroWriteAkkaPekkoApp extends App {
  val inputDataAvroSchema = SchemaBuilder
    .record("data")
    .namespace("example")
    .fields()
    .requiredInt("i")
    .requiredString("text")
    .requiredString("partition")
    .endRecord()
  val partitionedDataAvroSchema = SchemaBuilder
    .record("data")
    .namespace("example")
    .fields()
    .requiredInt("i")
    .requiredString("text")
    .endRecord()

  val count = 100
  val data = (1 to count).map { i =>
    new GenericRecordBuilder(inputDataAvroSchema)
      .set("i", i)
      .set("text", Random.nextString(4))
      .set("partition", (i % 4).toString())
      .build()
  }
  val basePath = Path(Files.createTempDirectory("example"))
  val vcc      = ValueCodecConfiguration.Default

  implicit val system: ActorSystem = ActorSystem()
  import system.dispatcher

  lazy val writerFlow = ParquetStreams.viaParquet
    .custom[GenericRecord, AvroParquetWriter.Builder[GenericRecord]](path =>
      AvroParquetWriter
        .builder[GenericRecord](path.toOutputFile(ParquetWriter.Options()))
        .withSchema(partitionedDataAvroSchema)
    )
    .partitionUsing { case (path, record) =>
      val partitionValue = record.get("partition")
      val partitionedRecord = new GenericRecordBuilder(partitionedDataAvroSchema)
        .set("i", record.get("i"))
        .set("text", record.get("text"))
        .build()
      (path.append(s"partition=$partitionValue"), partitionedRecord)
    }
    .write(basePath)

  val stream = for {
    _ <- Source(data).via(writerFlow).runWith(Sink.ignore)
    _ <- ParquetStreams.fromParquet.generic
      .read(basePath)
      .runForeach(r =>
        println(
          s"i=${r.get[Int]("i", vcc)}, text=${r.get[String]("text", vcc)}, partition=${r.get[String]("partition", vcc)}"
        )
      )
  } yield ()

  stream.andThen { case _ =>
    system.terminate()
  }
}
