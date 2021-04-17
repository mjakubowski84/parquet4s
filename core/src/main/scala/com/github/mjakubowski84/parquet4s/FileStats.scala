package com.github.mjakubowski84.parquet4s

import org.apache.hadoop.fs.FileStatus
import org.apache.parquet.ParquetReadOptions
import org.apache.parquet.column.statistics.Statistics
import org.apache.parquet.hadoop.ParquetFileReader
import org.apache.parquet.hadoop.util.HadoopInputFile
import org.apache.parquet.schema.MessageType

import scala.collection.compat._
import scala.jdk.CollectionConverters._

/**
 * Calculates statistics from <b>unfiltered</b> Parquet files.
 */
private[parquet4s] class FileStats(
                                    status: FileStatus,
                                    options: ParquetReader.Options,
                                    projectionSchemaOpt: Option[MessageType]
                                  ) extends Stats {

  private val vcc = options.toValueCodecConfiguration
  private val inputFile = HadoopInputFile.fromStatus(status, options.hadoopConf)
  private val readerOptions = ParquetReadOptions.builder().build()

  private abstract class StatsReader {
    protected val reader: ParquetFileReader = ParquetFileReader.open(inputFile, readerOptions)
    projectionSchemaOpt.foreach(reader.setRequestedSchema)
    def close(): Unit = reader.close()
  }

  private class RecordCountReader extends StatsReader {
    def recordCount: Long = reader.getRecordCount
  }

  private class MinMaxReader[V](columnPath: String, currentExtreme: Option[V])
                            (implicit codec: ValueCodec[V], ordering: Ordering[V]) extends StatsReader {

    private def extreme(statsValue: Statistics[_] => IterableOnce[Value], choose: (V, V) => V) =
      reader.getRowGroups.asScala.iterator
        .map(block => block.getColumns.asScala.find(_.getPath.toDotString == columnPath))
        .collect { case Some(column) => column }
        .map(_.getStatistics)
        .flatMap(statsValue)
        .map(value => codec.decode(value, vcc))
        .foldLeft(currentExtreme) {
          case (None, v) => Option(v)
          case (Some(a), b) => Option(choose(a, b))
        }

    def min: Option[V] = extreme(statsMinValue, ordering.min)
    def max: Option[V] = extreme(statsMaxValue, ordering.max)

  }

  override def recordCount: Long = {
    val reader = new RecordCountReader
    try {
      reader.recordCount
    } finally {
      reader.close()
    }
  }

  override def min[V](columnPath: String, currentMin: Option[V])
                     (implicit codec: ValueCodec[V], ordering: Ordering[V]): Option[V] = {
    val reader = new MinMaxReader[V](columnPath, currentMin)
    try {
      reader.min
    } finally {
      reader.close()
    }
  }

  override def max[V](columnPath: String, currentMax: Option[V])
                     (implicit codec: ValueCodec[V], ordering: Ordering[V]): Option[V] = {
    val reader = new MinMaxReader[V](columnPath, currentMax)
    try {
      reader.max
    } finally {
      reader.close()
    }
  }
}
