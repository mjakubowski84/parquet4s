package com.github.mjakubowski84.parquet4s

import akka.Done
import akka.stream.scaladsl.{Flow, Keep, Sink}
import org.apache.hadoop.fs.Path
import org.slf4j.{Logger, LoggerFactory}

import scala.concurrent.Future

object SingleFileParquetSink {

  protected val logger: Logger = LoggerFactory.getLogger(this.getClass)

  def apply[T: ParquetRecordEncoder : ParquetSchemaResolver](path: Path,
                                                             options: ParquetWriter.Options = ParquetWriter.Options()
                                                            ): Sink[T, Future[Done]] = {
    val schema = ParquetSchemaResolver.resolveSchema[T]
    val writer = ParquetWriter.internalWriter(path, schema, options)
    val isDebugEnabled = logger.isDebugEnabled

    Flow[T]
      .map(ParquetRecordEncoder.encode[T])
      .fold(0) { case (acc, record) => writer.write(record); acc + 1}
      .map { count =>
        if (isDebugEnabled) logger.debug(s"$count records were successfully written to $path")
        writer.close()
      }
      .toMat(Sink.ignore)(Keep.right)
  }

}
