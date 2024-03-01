package com.github.mjakubowski84.parquet4s.stats

import com.github.mjakubowski84.parquet4s._

private[parquet4s] class PartitionedFileStats(wrapped: Stats, partitionView: PartitionView) extends Stats {
  override def recordCount = wrapped.recordCount

  override protected[parquet4s] def min[V](columnPath: ColumnPath, currentMin: Option[V])(implicit
      decoder: ValueDecoder[V],
      ordering: Ordering[V]
  ): Option[V] =
    (partitionView.value(columnPath).map(_.toStringUsingUTF8.asInstanceOf[V]), currentMin) match {
      case (Some(partitionValue), Some(cm)) => Some(ordering.min(partitionValue, cm))
      case (Some(partitionValue), None)     => Some(partitionValue)
      case _                                => wrapped.min[V](columnPath, currentMin)
    }

  override protected[parquet4s] def max[V](columnPath: ColumnPath, currentMax: Option[V])(implicit
      decoder: ValueDecoder[V],
      ordering: Ordering[V]
  ): Option[V] =
    (partitionView.value(columnPath).map(_.toStringUsingUTF8.asInstanceOf[V]), currentMax) match {
      case (Some(partitionValue), Some(cm)) => Some(ordering.max(partitionValue, cm))
      case (Some(partitionValue), None)     => Some(partitionValue)
      case _                                => wrapped.max[V](columnPath, currentMax)
    }
}
