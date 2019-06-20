package com.github.mjakubowski84.parquet4s

import java.nio.file.Paths
import java.sql.Timestamp
import java.util.UUID

import akka.kafka.CommitterSettings
import akka.kafka.ConsumerMessage.CommittableOffsetBatch
import akka.kafka.scaladsl.Committer
import akka.stream.scaladsl.{Flow, Keep}
import com.github.mjakubowski84.parquet4s.IndefiniteStreamParquetSink.ChunkPathBuilder
import com.google.common.io.Files
import org.apache.hadoop.fs.Path
import org.apache.parquet.hadoop.metadata.CompressionCodecName

import scala.concurrent.duration._

object MessageSink {

  case class Data(timestamp: Timestamp, word: String)

}

trait MessageSink {

  this: Akka =>

  import MessageSink._
  import MessageSource._

  val baseWritePath: String = Paths.get(Files.createTempDir().getAbsolutePath, "messages").toString

  private val writerOptions = ParquetWriter.Options(compressionCodecName = CompressionCodecName.SNAPPY)

  private lazy val committerSink = Flow.apply[Seq[Message]].map { messages =>
    CommittableOffsetBatch(messages.map(_.committableOffset))
  }.toMat(Committer.sink(CommitterSettings(system)))(Keep.right)

  def chunkPath: ChunkPathBuilder[Message] = {
    case (basePath, chunk) =>
      val lastElementDateTime = new Timestamp(chunk.last.record.timestamp()).toLocalDateTime
      val year = lastElementDateTime.getYear
      val month = lastElementDateTime.getMonthValue
      val day = lastElementDateTime.getDayOfMonth
      val uuid = UUID.randomUUID()

      basePath.suffix(s"/$year/$month/$day/part-$uuid.parquet")
  }

  lazy val messageSink = IndefiniteStreamParquetSink(
    path = new Path(baseWritePath),
    maxChunkSize = 128,
    chunkWriteTimeWindow = 10.seconds,
    buildChunkPath = chunkPath,
    preWriteTransformation = { message: Message =>
      Data(
        timestamp = new Timestamp(message.record.timestamp()),
        word = message.record.value()
      )
    },
    postWriteSink = committerSink,
    options = writerOptions
  )

}
