package com.github.mjakubowski84.parquet4s

import akka.Done
import akka.stream.scaladsl.{Flow, Keep, Sink}
import org.apache.parquet.schema.MessageType
import org.slf4j.{Logger, LoggerFactory}

import scala.concurrent.Future
import scala.util.Try
import scala.util.control.NonFatal

object SingleFileParquetSink {

  trait ToParquet {
    def of[T: ParquetSchemaResolver: ParquetRecordEncoder]: Builder[T]
    def generic(schema: MessageType): Builder[RowParquetRecord]
  }

  private[parquet4s] object ToParquetImpl extends ToParquet {
    override def of[T: ParquetSchemaResolver : ParquetRecordEncoder]: Builder[T] =
      BuilderImpl()
    override def generic(schema: MessageType): Builder[RowParquetRecord] =
      BuilderImpl()(
        schemaResolver = RowParquetRecord.genericParquetSchemaResolver(schema),
        encoder = RowParquetRecord.genericParquetRecordEncoder
      )
  }

  trait Builder[T] {
    def options(options: ParquetWriter.Options): Builder[T]
    // TODO build or write?
    def build(path: Path): Sink[T, Future[Done]]
  }

  private case class BuilderImpl[T](options: ParquetWriter.Options = ParquetWriter.Options())
                                   (implicit
                                    schemaResolver: ParquetSchemaResolver[T],
                                    encoder: ParquetRecordEncoder[T]
                                   ) extends Builder[T] {
    override def options(options: ParquetWriter.Options): Builder[T] = this.copy(options = options)
    override def build(path: Path): Sink[T, Future[Done]] = apply(path, options)
  }

  private val logger: Logger = LoggerFactory.getLogger(this.getClass)

  private def apply[T: ParquetRecordEncoder : ParquetSchemaResolver](path: Path,
                                                                     options: ParquetWriter.Options = ParquetWriter.Options()
                                                                    ): Sink[T, Future[Done]] = {
    val schema = ParquetSchemaResolver.resolveSchema[T]
    val writer = ParquetWriter.internalWriter(path, schema, options)
    val valueCodecConfiguration = ValueCodecConfiguration(options)

    def encode(data: T): RowParquetRecord = ParquetRecordEncoder.encode[T](data, valueCodecConfiguration)

    Flow[T]
      .map(encode)
      .fold(0) { case (acc, record) => writer.write(record); acc + 1}
      .map { count =>
        if (logger.isDebugEnabled) logger.debug(s"$count records were successfully written to $path")
        writer.close()
      }
      .recover { case NonFatal(e) =>
        Try(writer.close())
        throw e
      }
      .toMat(Sink.ignore)(Keep.right)
  }

}
