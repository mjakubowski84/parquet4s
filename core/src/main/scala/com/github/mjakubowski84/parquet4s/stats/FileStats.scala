package com.github.mjakubowski84.parquet4s.stats

import com.github.mjakubowski84.parquet4s.*
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileStatus
import org.apache.parquet.ParquetReadOptions
import org.apache.parquet.column.statistics.Statistics
import org.apache.parquet.hadoop.ParquetFileReader
import org.apache.parquet.hadoop.util.HadoopInputFile
import org.apache.parquet.io.InputFile
import org.apache.parquet.schema.MessageType

import scala.collection.compat.*
import scala.jdk.CollectionConverters.*
import scala.util.Using

private[parquet4s] object FileStats {
  def apply(
      status: FileStatus,
      hadoopConf: Configuration,
      vcc: ValueCodecConfiguration,
      projectionSchemaOpt: Option[MessageType]
  ): Stats =
    new FileStats(HadoopInputFile.fromStatus(status, hadoopConf), vcc, projectionSchemaOpt)
}

/** Calculates statistics from <b>unfiltered</b> Parquet files.
  */
private[parquet4s] class FileStats(
    inputFile: InputFile,
    vcc: ValueCodecConfiguration,
    projectionSchemaOpt: Option[MessageType]
) extends Stats {

  private val readerOptions = ParquetReadOptions.builder().build()

  abstract private class StatsReader extends AutoCloseable {
    protected val reader: ParquetFileReader = ParquetFileReader.open(inputFile, readerOptions)
    projectionSchemaOpt.foreach(reader.setRequestedSchema)
    override def close(): Unit = reader.close()
  }

  private class RecordCountReader extends StatsReader {
    def recordCount: Long = reader.getRecordCount
  }

  private class MinMaxReader[V](columnPath: ColumnPath, currentExtreme: Option[V])(implicit
      decoder: ValueDecoder[V],
      ordering: Ordering[V]
  ) extends StatsReader {
    private val dotString = columnPath.toString

    private def extreme(statsValue: Statistics[?] => IterableOnce[Value], choose: (V, V) => V) =
      reader.getRowGroups.asScala.iterator
        .map(block => block.getColumns.asScala.find(_.getPath.toDotString == dotString))
        .collect { case Some(column) => column }
        .map(_.getStatistics)
        .flatMap(statsValue)
        .map(value => decoder.decode(value, vcc))
        .foldLeft(currentExtreme) {
          case (None, v)    => Option(v)
          case (Some(a), b) => Option(choose(a, b))
        }

    def min: Option[V] = extreme(statsMinValue, ordering.min)
    def max: Option[V] = extreme(statsMaxValue, ordering.max)

  }

  override def recordCount: Long =
    Using.resource(new RecordCountReader)(_.recordCount)

  override def min[V](columnPath: ColumnPath, currentMin: Option[V])(implicit
      decoder: ValueDecoder[V],
      ordering: Ordering[V]
  ): Option[V] =
    Using.resource(new MinMaxReader[V](columnPath, currentMin))(_.min)

  override def max[V](columnPath: ColumnPath, currentMax: Option[V])(implicit
      decoder: ValueDecoder[V],
      ordering: Ordering[V]
  ): Option[V] =
    Using.resource(new MinMaxReader[V](columnPath, currentMax))(_.max)
  
}
