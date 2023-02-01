package com.github.mjakubowski84.parquet4s.akka

import akka.actor.ActorSystem
import akka.stream.scaladsl.Source
import com.github.mjakubowski84.parquet4s.{ParquetStreams, Path}
import com.github.mjakubowski84.parquet4s.protobuf.DataOuterClass.Data
import org.apache.parquet.proto.ProtoParquetWriter

import java.nio.file.Files
import scala.util.Random

object CustomParquetWriterAkkaApp extends App {
  val count = 100
  val data  = (1 to count).map(i => Data.newBuilder.setId(i).setText(Random.nextString(4)).build)
  val path  = Path(Files.createTempDirectory("example"))

  implicit val system: ActorSystem = ActorSystem()

  import system.dispatcher

  val builder = ProtoParquetWriter.builder[Data](path.append("data.parquet").hadoopPath).withMessage(classOf[Data])

  val sink = ParquetStreams.toParquetSingleFile
    .custom[Data, ProtoParquetWriter.Builder[Data]](builder)
    .write

  for {
    _ <- Source(data).runWith(sink)
    _ <- system.terminate()
  } yield ()
}
