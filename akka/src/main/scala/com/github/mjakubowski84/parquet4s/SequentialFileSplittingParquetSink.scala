package com.github.mjakubowski84.parquet4s

import akka.Done
import akka.stream.scaladsl.{Flow, Keep, Sink}
import org.apache.hadoop.fs.Path
import org.apache.parquet.schema.MessageType
import org.slf4j.{Logger, LoggerFactory}

import scala.concurrent.Future

object SequentialFileSplittingParquetSink extends IOOps {

  protected val logger: Logger = LoggerFactory.getLogger(this.getClass)

  def apply[T: ParquetRecordEncoder : ParquetSchemaResolver](path: Path,
                                                             maxRecordsPerFile: Long,
                                                             options: ParquetWriter.Options = ParquetWriter.Options()
                                                            ): Sink[T, Future[Done]] = {
    val schema = ParquetSchemaResolver.resolveSchema[T]
    val valueCodecConfiguration = options.toValueCodecConfiguration

    validateWritePath(path, options)

    def encode(data: T): RowParquetRecord = ParquetRecordEncoder.encode[T](data, valueCodecConfiguration)

    Flow[T]
      .zipWithIndex
      .map { case (elem, index) => OrderedChunkElem(encode(elem), index) }
      .fold(OrderedChunk(path, schema, maxRecordsPerFile, options))(_.write(_))
      .map(_.close())
      .toMat(Sink.ignore)(Keep.right)
  }

  private case class OrderedChunkElem(record: RowParquetRecord, index: Long) {
    def isSplit(maxRecordsPerFile: Long): Boolean = index % maxRecordsPerFile == 0
  }

  private trait OrderedChunk {
    def write(elem: OrderedChunkElem): OrderedChunk
    def close(): Unit
  }

  private object OrderedChunk {

    def apply(basePath: Path,
              schema: MessageType,
              maxRecordsPerFile: Long,
              options: ParquetWriter.Options): OrderedChunk = new PendingOrderedChunk(basePath, schema, maxRecordsPerFile, options)


    private[OrderedChunk] class PendingOrderedChunk(basePath: Path,
                                                    schema: MessageType,
                                                    maxRecordsPerFile: Long,
                                                    options: ParquetWriter.Options) extends OrderedChunk {
      override def write(elem: OrderedChunkElem): OrderedChunk = {
        val chunkNumber: Int = Math.floorDiv(elem.index, maxRecordsPerFile).toInt
        val chunkPath = Path.mergePaths(basePath, new Path(chunkFileName(chunkNumber)))
        val writer = ParquetWriter.internalWriter(chunkPath, schema, options)
        writer.write(elem.record)
        new StartedOrderedChunk(basePath, schema, maxRecordsPerFile, options, chunkPath, writer, acc = 1)
      }

      override def close(): Unit = ()

      private def chunkFileName(chunkNumber: Int): String = f"/part-$chunkNumber%05d.parquet"
    }

    private[OrderedChunk] class StartedOrderedChunk(basePath: Path,
                                                    schema: MessageType,
                                                    maxRecordsPerFile: Long,
                                                    options: ParquetWriter.Options,
                                                    chunkPath: Path,
                                                    writer: ParquetWriter.InternalWriter,
                                                    acc: Long) extends OrderedChunk {
      override def write(elem: OrderedChunkElem): OrderedChunk = {
        if (elem.isSplit(maxRecordsPerFile)) {
          this.close()
          new PendingOrderedChunk(basePath, schema, maxRecordsPerFile, options).write(elem)
        } else {
          writer.write(elem.record)
          new StartedOrderedChunk(basePath, schema, maxRecordsPerFile, options, chunkPath, writer, acc = acc + 1)
        }
      }

      override def close(): Unit = {
        if (logger.isDebugEnabled) logger.debug(s"$acc records were successfully written to $chunkPath")
        writer.close()
      }
    }
  }

}
