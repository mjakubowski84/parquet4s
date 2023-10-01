package com.github.mjakubowski84.parquet4s.stats

import com.github.mjakubowski84.parquet4s.*
import org.apache.hadoop.conf.Configuration
import org.apache.parquet.filter2.compat.FilterCompat
import org.apache.parquet.filter2.compat.FilterCompat.NoOpFilter
import org.apache.parquet.hadoop.util.HadoopInputFile
import org.apache.parquet.io.InputFile
import org.apache.parquet.schema.MessageType

private[parquet4s] class LazyDelegateStats(
    inputFile: InputFile,
    vcc: ValueCodecConfiguration,
    hadoopConf: Configuration,
    projectionSchemaOpt: Option[MessageType],
    filter: FilterCompat.Filter
) extends Stats {
  private lazy val delegate: Stats = {
    inputFile match {
      case hadoopInputFile: HadoopInputFile =>
        val hadoopPath = hadoopInputFile.getPath
        val fs         = hadoopPath.getFileSystem(hadoopConf)
        val statsArray = fs.listStatus(hadoopPath).map {
          case status if filter.isInstanceOf[NoOpFilter] =>
            FileStats(status, hadoopConf, vcc, projectionSchemaOpt)
          case status =>
            FilteredFileStats(status, hadoopConf, vcc, projectionSchemaOpt, filter)
        }
        if (statsArray.length == 1) statsArray.head
        else new CompoundStats(statsArray)
      case _ if filter.isInstanceOf[NoOpFilter] =>
        new FileStats(inputFile, vcc, projectionSchemaOpt)
      case _ =>
        new FilteredFileStats(inputFile, vcc, projectionSchemaOpt, filter)

    }

  }

  override def recordCount: Long = delegate.recordCount

  override protected[parquet4s] def min[V](columnPath: ColumnPath, currentMin: Option[V])(implicit
      decoder: ValueDecoder[V],
      ordering: Ordering[V]
  ): Option[V] =
    delegate.min(columnPath, currentMin)

  override protected[parquet4s] def max[V](columnPath: ColumnPath, currentMax: Option[V])(implicit
      decoder: ValueDecoder[V],
      ordering: Ordering[V]
  ): Option[V] =
    delegate.max(columnPath, currentMax)
}
