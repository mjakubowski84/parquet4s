package com.github.mjakubowski84.parquet4s.akkaPekko.indefinite

import com.github.mjakubowski84.parquet4s.ScalaCompat.Done
import com.github.mjakubowski84.parquet4s.ScalaKafkaCompat.kafka.CommitterSettings
import com.github.mjakubowski84.parquet4s.ScalaKafkaCompat.kafka.ConsumerMessage.CommittableOffsetBatch
import com.github.mjakubowski84.parquet4s.ScalaKafkaCompat.kafka.scaladsl.Committer
import com.github.mjakubowski84.parquet4s.ScalaCompat.stream.FlowShape
import com.github.mjakubowski84.parquet4s.ScalaCompat.stream.scaladsl.{Flow, Keep, Sink}
import com.github.mjakubowski84.parquet4s.ScalaCompat.stream.stage.GraphStage
import com.github.mjakubowski84.parquet4s.{Col, ParquetStreams, ParquetWriter, Path}
import org.apache.parquet.hadoop.metadata.CompressionCodecName

import java.nio.file.Files
import java.sql.Timestamp
import scala.concurrent.Future
import scala.concurrent.duration.*

object MessageSink {

  case class Data(
      year: String,
      month: String,
      day: String,
      timestamp: Timestamp,
      word: String
  )

  val MaxChunkSize: Int                    = 128
  val ChunkWriteTimeWindow: FiniteDuration = 10.seconds
  val WriteDirectoryName: String           = "messages"

}

trait MessageSink {

  this: AkkaPekko & Logger =>

  import MessageSink.*
  import MessageSource.*

  protected val baseWritePath: Path = Path(Files.createTempDirectory("example")).append(WriteDirectoryName)

  private val writerOptions = ParquetWriter.Options(compressionCodecName = CompressionCodecName.SNAPPY)

  lazy val messageSink: Sink[Message, Future[Done]] =
    Flow[Message]
      .via(saveDataToParquetFlow)
      .map(_.committableOffset)
      .grouped(MaxChunkSize)
      .map(CommittableOffsetBatch.apply)
      .toMat(Committer.sink(CommitterSettings(system)))(Keep.right)

  private lazy val saveDataToParquetFlow: GraphStage[FlowShape[Message, Message]] =
    ParquetStreams.viaParquet
      .of[Message]
      .preWriteTransformation { message =>
        val timestamp     = new Timestamp(message.record.timestamp())
        val localDateTime = timestamp.toLocalDateTime
        Some(
          Data(
            year      = localDateTime.getYear.toString,
            month     = localDateTime.getMonthValue.toString,
            day       = localDateTime.getDayOfMonth.toString,
            timestamp = timestamp,
            word      = message.record.value()
          )
        )
      }
      .partitionBy(Col("year"), Col("month"), Col("day"))
      .maxCount(MaxChunkSize.toLong)
      .maxDuration(ChunkWriteTimeWindow)
      .options(writerOptions)
      .postWriteHandler { state =>
        logger.info(s"Just wrote to ${state.modifiedPartitions}")
      }
      .write(baseWritePath)

}
