package com.github.mjakubowski84.parquet4s

import java.sql.Timestamp
import java.util.UUID

import akka.Done
import akka.kafka.CommitterSettings
import akka.kafka.ConsumerMessage.CommittableOffsetBatch
import akka.kafka.scaladsl.Committer
import akka.stream.scaladsl.{Flow, Keep, Sink}
import com.google.common.io.Files
import org.apache.hadoop.fs.Path
import org.apache.parquet.hadoop.metadata.CompressionCodecName

import scala.concurrent.Future
import scala.concurrent.duration._

object MessageSink {

  case class Data(timestamp: Timestamp, word: String)

  val MaxChunkSize: Int = 128
  val ChunkWriteTimeWindow: FiniteDuration = 10.seconds
  val WriteDirectoryName: String = "messages"

}

trait MessageSink {

  this: Akka =>

  import MessageSink._
  import MessageSource._

  protected val baseWritePath: String = new Path(Files.createTempDir().getAbsolutePath, WriteDirectoryName).toString

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

  lazy val messageSink: Sink[Message, Future[Done]] = ParquetStreams.toParquetIndefinite(
    path = baseWritePath,
    maxChunkSize = MaxChunkSize,
    chunkWriteTimeWindow = ChunkWriteTimeWindow,
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
