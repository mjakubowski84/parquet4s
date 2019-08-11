package com.github.mjakubowski84.parquet4s

import java.io.Closeable

import org.apache.hadoop.fs.Path
import org.slf4j.LoggerFactory

/**
  * Interface for a writer which can handle multiple calls to .write with a single call to .close.
  * @tparam T schema of data to write
  */
trait IncrementalParquetWriter[T] extends Closeable {
  def write(data: Iterable[T])
}

object IncrementalParquetWriter {

  /**
    * Default instance of an [[IncrementalParquetWriter]]
    * @param path location where files are meant to be written
    * @param options configuration of how Parquet files should be created and written
    * @tparam T schema of data to write
    */
  def apply[T: ParquetRecordEncoder: ParquetSchemaResolver](
                                                             path: String,
                                                             options: ParquetWriter.Options = ParquetWriter.Options()
                                                           ): IncrementalParquetWriter[T] =
    new IncrementalParquetWriter[T] {
      private val writer = ParquetWriter.internalWriter(
        path = new Path(path),
        schema = ParquetSchemaResolver.resolveSchema[T],
        options = options
      )
      private val valueCodecConfiguration = options.toValueCodecConfiguration
      private val logger = LoggerFactory.getLogger(this.getClass)
      private var closed = false

      override def write(data: Iterable[T]): Unit = {
        if (closed) {
          throw new IllegalStateException("Attempted to write with a writer which was already closed")
        } else {
          data.foreach { elem =>
            writer.write(ParquetRecordEncoder.encode[T](elem, valueCodecConfiguration))
          }
        }
      }

      override def close(): Unit = synchronized {
        if (closed) {
          logger.warn("Attempted to close a writer which was already closed")
        } else {
          closed = true
          writer.close()
        }
      }
      
    }

}
