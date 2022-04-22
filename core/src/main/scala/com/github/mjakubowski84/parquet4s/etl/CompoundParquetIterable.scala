package com.github.mjakubowski84.parquet4s.etl

import com.github.mjakubowski84.parquet4s.*
import com.github.mjakubowski84.parquet4s.stats.CompoundStats

private[parquet4s] class CompoundParquetIterable[T](components: Seq[ParquetIterable[T]]) extends ParquetIterable[T] {

  override val stats = new CompoundStats(components.map(_.stats))

  override lazy val valueCodecConfiguration: ValueCodecConfiguration =
    components.headOption.map(_.valueCodecConfiguration).getOrElse(ValueCodecConfiguration.Default)

  override def iterator: Iterator[T] =
    components.foldLeft[Iterator[T]](Iterator.empty)(_ ++ _.iterator)

  override def close(): Unit = components.foreach(_.close())

  override private[parquet4s] def appendTransformation(transformation: RowParquetRecord => Iterable[RowParquetRecord]) =
    new CompoundParquetIterable[T](components.map(_.appendTransformation(transformation)))

  override private[parquet4s] def changeDecoder[U: ParquetRecordDecoder]: ParquetIterable[U] =
    new CompoundParquetIterable[U](components.map(_.changeDecoder[U]))

  override def concat(other: ParquetIterable[T]): ParquetIterable[T] =
    new CompoundParquetIterable(components :+ other)
}
