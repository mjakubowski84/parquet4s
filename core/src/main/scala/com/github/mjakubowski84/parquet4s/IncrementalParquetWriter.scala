package com.github.mjakubowski84.parquet4s

import java.io.Closeable

import org.apache.hadoop.fs.Path

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
  def apply[T: ParquetRecordEncoder : ParquetSchemaResolver](path: String, options: ParquetWriter.Options = ParquetWriter.Options()): IncrementalParquetWriter[T] = {
    new IncrementalParquetWriter[T] {
      private val writer = ParquetWriter.internalWriter(new Path(path), ParquetSchemaResolver.resolveSchema[T], options)
      private val valueCodecConfiguration = options.toValueCodecConfiguration
      override def write(data: Iterable[T]): Unit =
        data.foreach { elem =>
          writer.write(ParquetRecordEncoder.encode[T](elem, valueCodecConfiguration))
        }
      override def close(): Unit = writer.close()
    }
  }
}

