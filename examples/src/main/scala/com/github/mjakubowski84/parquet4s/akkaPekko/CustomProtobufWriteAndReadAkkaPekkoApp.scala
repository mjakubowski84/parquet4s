package com.github.mjakubowski84.parquet4s.akkaPekko

import com.github.mjakubowski84.parquet4s.ScalaCompat.actor.ActorSystem
import com.github.mjakubowski84.parquet4s.ScalaCompat.stream.scaladsl.Source
import com.github.mjakubowski84.parquet4s.{ParquetStreams, Path}
import com.github.mjakubowski84.parquet4s.protobuf.DataOuterClass.Data
import org.apache.parquet.proto.ProtoParquetWriter

import java.nio.file.Files
import scala.util.Random
import org.apache.parquet.proto.ProtoParquetReader
import com.google.protobuf.TextFormat

object CustomProtobufWriteAndReadAkkaPekkoApp extends App {
  val count = 100
  val data  = (1 to count).map(i => Data.newBuilder.setId(i).setText(Random.nextString(4)).build)
  val path  = Path(Files.createTempDirectory("example"))

  implicit val system: ActorSystem = ActorSystem()

  import system.dispatcher

  lazy val writerBuilder =
    ProtoParquetWriter.builder[Data](path.append("data.parquet").hadoopPath).withMessage(classOf[Data])

  lazy val writerSink = ParquetStreams.toParquetSingleFile
    .custom[Data, ProtoParquetWriter.Builder[Data]](writerBuilder)
    .write

  lazy val readerBuilder = ProtoParquetReader.builder[Data.Builder](path.hadoopPath)

  lazy val readerSource = ParquetStreams.fromParquet
    .custom[Data.Builder](readerBuilder)
    .read[Data](_.build())

  val stream = for {
    _ <- Source(data).runWith(writerSink)
    _ <- readerSource.runForeach(data => println(TextFormat.printer().escapingNonAscii(false).printToString(data)))
  } yield ()

  stream.andThen {
    // finish
    case _ => system.terminate()
  }
}
