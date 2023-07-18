package com.github.mjakubowski84.parquet4s.stats

import com.github.mjakubowski84.parquet4s.*
import org.apache.hadoop.conf.Configuration
import org.apache.parquet.filter2.compat.FilterCompat
import org.apache.parquet.filter2.compat.FilterCompat.NoOpFilter
import org.apache.parquet.schema.MessageType

private[parquet4s] class LazyDelegateStats(
    path: Path,
    vcc: ValueCodecConfiguration,
    hadoopConf: Configuration,
    projectionSchemaOpt: Option[MessageType],
    filter: FilterCompat.Filter
) extends Stats {
  private lazy val delegate: Stats = {
    val fs = path.toHadoop.getFileSystem(hadoopConf)
    val statsArray = fs.listStatus(path.toHadoop).map {
      case status if filter.isInstanceOf[NoOpFilter] =>
        FileStats(status, vcc, hadoopConf, projectionSchemaOpt)
      case status =>
        new FilteredFileStats(status, vcc, hadoopConf, projectionSchemaOpt, filter)
    }
    if (statsArray.length == 1) statsArray.head
    else new CompoundStats(statsArray.toIndexedSeq)
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
