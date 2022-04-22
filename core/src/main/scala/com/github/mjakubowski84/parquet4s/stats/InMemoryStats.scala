package com.github.mjakubowski84.parquet4s.stats

import com.github.mjakubowski84.parquet4s.*

private[parquet4s] class InMemoryStats(iterable: Iterable[RowParquetRecord], vcc: ValueCodecConfiguration)
    extends Stats {

  override lazy val recordCount: Long = iterable.size

  override protected[parquet4s] def min[V](columnPath: ColumnPath, currentMin: Option[V])(implicit
      decoder: ValueDecoder[V],
      ordering: Ordering[V]
  ): Option[V] = iterable.foldLeft(currentMin) { case (currOpt, record) =>
    (record.get(columnPath).map(decoder.decode(_, vcc)), currOpt) match {
      case (Some(v), Some(curr)) => Some(ordering.min(curr, v))
      case (Some(v), None)       => Some(v)
      case (None, _)             => currOpt
    }
  }

  override protected[parquet4s] def max[V](columnPath: ColumnPath, currentMax: Option[V])(implicit
      decoder: ValueDecoder[V],
      ordering: Ordering[V]
  ): Option[V] = iterable.foldLeft(currentMax) { case (currOpt, record) =>
    (record.get(columnPath).map(decoder.decode(_, vcc)), currOpt) match {
      case (Some(v), Some(curr)) => Some(ordering.max(curr, v))
      case (Some(v), None)       => Some(v)
      case (None, _)             => currOpt
    }
  }
}
