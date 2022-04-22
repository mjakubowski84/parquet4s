package com.github.mjakubowski84.parquet4s.etl

import com.github.mjakubowski84.parquet4s.*
import com.github.mjakubowski84.parquet4s.stats.InMemoryStats

private[parquet4s] class InMemoryParquetIterable[T](
    data: => Iterable[RowParquetRecord],
    override val valueCodecConfiguration: ValueCodecConfiguration        = ValueCodecConfiguration.Default,
    transformations: Seq[RowParquetRecord => Iterable[RowParquetRecord]] = Seq.empty,
    decode: RowParquetRecord => T                                        = identity[RowParquetRecord] _
) extends ParquetIterable[T] {

  override private[parquet4s] def appendTransformation(transformation: RowParquetRecord => Iterable[RowParquetRecord]) =
    new InMemoryParquetIterable[T](
      data                    = data,
      valueCodecConfiguration = valueCodecConfiguration,
      transformations         = transformations :+ transformation,
      decode                  = decode
    )

  override private[parquet4s] def changeDecoder[U: ParquetRecordDecoder] =
    new InMemoryParquetIterable[U](
      data                    = data,
      valueCodecConfiguration = valueCodecConfiguration,
      transformations         = transformations,
      decode                  = record => ParquetRecordDecoder.decode[U](record, valueCodecConfiguration)
    )

  override private[parquet4s] lazy val stats = new InMemoryStats(data, valueCodecConfiguration)

  override def close(): Unit = ()

  override def iterator: Iterator[T] =
    if (transformations.isEmpty) data.iterator.map(decode)
    else
      data.iterator.flatMap(record =>
        transformations
          .foldLeft(Iterator(record)) { case (iterator, transformation) =>
            iterator.flatMap(transformation)
          }
          .map(decode)
      )
}
