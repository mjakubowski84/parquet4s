package com.github.mjakubowski84.parquet4s

import akka.Done
import akka.stream.scaladsl.{Flow, Keep, Sink}
import org.apache.hadoop.fs.Path
import org.slf4j.{Logger, LoggerFactory}

import scala.concurrent.Future
import scala.util.Try
import scala.util.control.NonFatal

private[parquet4s] object SingleFileParquetSink {

  protected val logger: Logger = LoggerFactory.getLogger(this.getClass)

  def apply[T: ParquetRecordEncoder : ParquetSchemaResolver](path: Path,
                                                             options: ParquetWriter.Options = ParquetWriter.Options()
                                                            ): Sink[T, Future[Done]] = {
    val schema = ParquetSchemaResolver.resolveSchema[T]
    val writer = ParquetWriter.internalWriter(path, schema, options)
    val valueCodecConfiguration = options.toValueCodecConfiguration
    val isDebugEnabled = logger.isDebugEnabled

    def encode(data: T): RowParquetRecord = ParquetRecordEncoder.encode[T](data, valueCodecConfiguration)

    Flow[T]
      .map(encode)
      .fold(0) { case (acc, record) => writer.write(record); acc + 1}
      .map { count =>
        if (isDebugEnabled) logger.debug(s"$count records were successfully written to $path")
        writer.close()
      }
      .recover { case NonFatal(e) =>
        Try(writer.close())
        throw e
      }
      .toMat(Sink.ignore)(Keep.right)
  }

}
