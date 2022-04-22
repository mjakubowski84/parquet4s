package com.github.mjakubowski84.parquet4s.stats

import com.github.mjakubowski84.parquet4s.{ColumnPath, Stats, ValueDecoder}

/** Calculates [[Stats]] from multiple files.
  */
private[parquet4s] class CompoundStats(statsSeq: Seq[Stats]) extends Stats {

  override lazy val recordCount: Long = statsSeq.map(_.recordCount).sum

  override def min[V](columnPath: ColumnPath, currentMin: Option[V])(implicit
      decoder: ValueDecoder[V],
      ordering: Ordering[V]
  ): Option[V] =
    statsSeq.foldLeft(currentMin) { case (acc, stats) =>
      stats.min(columnPath, acc)
    }

  override def max[V](columnPath: ColumnPath, currentMax: Option[V])(implicit
      decoder: ValueDecoder[V],
      ordering: Ordering[V]
  ): Option[V] =
    statsSeq.foldLeft(currentMax) { case (acc, stats) =>
      stats.max(columnPath, acc)
    }

}
