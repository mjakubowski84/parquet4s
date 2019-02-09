package com.github.mjakubowski84.parquet4s

import java.util.UUID

import akka.Done
import akka.stream.scaladsl.{Flow, Keep, Sink}
import org.apache.hadoop.fs.Path
import org.apache.parquet.schema.MessageType
import org.slf4j.{Logger, LoggerFactory}

import scala.concurrent.Future

object UnorderedParallelParquetSink extends IOOps {

  protected val logger: Logger = LoggerFactory.getLogger(this.getClass)

  def apply[T: ParquetRecordEncoder : ParquetSchemaResolver](path: Path,
                                                             parallelism: Int,
                                                             options: ParquetWriter.Options = ParquetWriter.Options()
                                                            ): Sink[T, Future[Done]] = {
    val schema = ParquetSchemaResolver.resolveSchema[T]

    validateWritePath(path, options)

    Flow[T]
      .zipWithIndex
      .groupBy(parallelism, elemAndIndex => Math.floorMod(elemAndIndex._2, parallelism))
      .map(elemAndIndex => ParquetRecordEncoder.encode(elemAndIndex._1))
      .fold(UnorderedChunk(path, schema, options))(_.write(_))
      .map(_.close())
      .async
      .mergeSubstreamsWithParallelism(parallelism)
      .toMat(Sink.ignore)(Keep.right)
  }

  private trait UnorderedChunk {

    def write(record: RowParquetRecord): UnorderedChunk

    def close(): Unit

  }

  private object UnorderedChunk {

    def apply(basePath: Path,
              schema: MessageType,
              options: ParquetWriter.Options): UnorderedChunk = new PendingUnorderedChunk(basePath, schema, options)

    private[UnorderedChunk] class PendingUnorderedChunk(basePath: Path,
                                        schema: MessageType,
                                        options: ParquetWriter.Options) extends UnorderedChunk {
      override def write(record: RowParquetRecord): UnorderedChunk = {
        val chunkPath = Path.mergePaths(basePath, new Path(s"/part-${UUID.randomUUID()}.parquet"))
        val writer = ParquetWriter.internalWriter(chunkPath, schema, options)
        writer.write(record)
        new StartedUnorderedChunk(chunkPath, writer, acc = 1)
      }

      override def close(): Unit = ()
    }

    private[UnorderedChunk] class StartedUnorderedChunk(chunkPath: Path,
                                        writer: ParquetWriter.InternalWriter,
                                        acc: Long
                                       ) extends UnorderedChunk {
      override def write(record: RowParquetRecord): UnorderedChunk = {
        writer.write(record)
        new StartedUnorderedChunk(chunkPath, writer, acc = acc + 1)
      }

      override def close(): Unit = {
        if (logger.isDebugEnabled) logger.debug(s"$acc records were successfully written to $chunkPath")
        writer.close()
      }
    }
  }

}
