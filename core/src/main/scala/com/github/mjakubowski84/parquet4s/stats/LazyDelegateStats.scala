package com.github.mjakubowski84.parquet4s.stats

import com.github.mjakubowski84.parquet4s.*
import org.apache.parquet.filter2.compat.FilterCompat
import org.apache.parquet.filter2.compat.FilterCompat.NoOpFilter
import org.apache.parquet.io.InputFile
import org.apache.parquet.schema.MessageType

private[parquet4s] class LazyDelegateStats(
    inputFile: InputFile,
    vcc: ValueCodecConfiguration,
    projectionSchemaOpt: Option[MessageType],
    filter: FilterCompat.Filter,
    partitionViewOpt: Option[PartitionView]
) extends Stats {
  private lazy val delegate: Stats = {
    val fileStats =
      if (filter.isInstanceOf[NoOpFilter])
        new FileStats(inputFile, vcc, projectionSchemaOpt)
      else
        new FilteredFileStats(inputFile, vcc, projectionSchemaOpt, filter)
    partitionViewOpt match {
      case Some(partitionView) if partitionView.nonEmpty =>
        new PartitionedFileStats(fileStats, partitionView)
      case _ =>
        fileStats
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
