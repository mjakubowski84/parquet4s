package com.github.mjakubowski84.parquet4s
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.apache.hadoop.fs.Path
import akka.stream.scaladsl.Sink
import akka.stream.scaladsl.Flow
import scala.concurrent.Future
import akka.Done
import scala.concurrent.duration.FiniteDuration
import akka.stream.scaladsl.Keep
import java.util.UUID
import akka.NotUsed

object IndefiniteStreamParquetSink extends IOOps {

  protected val logger: Logger = LoggerFactory.getLogger(this.getClass)

  def apply[In, ToWrite: ParquetWriter](path: Path,
                              maxChunkSize: Int,
                              chunkWriteTimeWindow: FiniteDuration,
                              buildChunkPath: Path => Path = _.suffix(s"/part-${UUID.randomUUID()}.parquet"),
                              preWriteTransformation
                              postWriteFlow: Flow[Seq[T], Unit, NotUsed] = Flow.fromFunction { _: Seq[T] => () },
                              options: ParquetWriter.Options = ParquetWriter.Options()
                            ): Sink[T, Future[Done]] = {
    val valueCodecConfiguration = options.toValueCodecConfiguration

    validateWritePath(path, options)

    Flow[In]
      .groupedWithin(maxChunkSize, chunkWriteTimeWindow)
      .map { recordChunk =>
        val chunkPath = buildChunkPath(path)
        if (logger.isDebugEnabled()) logger.debug(s"Writing ${recordChunk.size} records to $chunkPath")
        ParquetWriter.write(chunkPath.toString(), recordChunk)
        recordChunk
      }
      .via(postWriteFlow)
      .toMat(Sink.ignore)(Keep.right)
  }

}
