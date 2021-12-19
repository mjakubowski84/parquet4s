package com.github.mjakubowski84.parquet4s

import java.io.Closeable

object ParquetIterable {

  private[parquet4s] def apply[T: ParquetRecordDecoder](
      iteratorFactory: () => Iterator[RowParquetRecord] & Closeable,
      valueCodecConfiguration: ValueCodecConfiguration,
      stats: Stats
  ): ParquetIterable[T] =
    new ParquetIterableImpl(
      iteratorFactory         = iteratorFactory,
      valueCodecConfiguration = valueCodecConfiguration,
      stats                   = stats,
      transformations         = Seq.empty,
      decode                  = record => ParquetRecordDecoder.decode(record, valueCodecConfiguration)
    )

  implicit class RowParquetRecordIterableOps(iterable: ParquetIterable[RowParquetRecord]) {

    // TODO docs
    def as[U: ParquetRecordDecoder]: ParquetIterable[U] = iterable.changeDecoder[U]

    // TODO docs
    @experimental
    def leftJoin(
        right: ParquetIterable[RowParquetRecord],
        onLeft: ColumnPath,
        onRight: ColumnPath
    ): ParquetIterable[RowParquetRecord] = Join.left(iterable, right, onLeft, onRight)

    @experimental
    def rightJoin(
        right: ParquetIterable[RowParquetRecord],
        onLeft: ColumnPath,
        onRight: ColumnPath
    ): ParquetIterable[RowParquetRecord] = Join.right(iterable, right, onLeft, onRight)

    @experimental
    def innerJoin(
        right: ParquetIterable[RowParquetRecord],
        onLeft: ColumnPath,
        onRight: ColumnPath
    ): ParquetIterable[RowParquetRecord] = Join.inner(iterable, right, onLeft, onRight)

    @experimental
    def fullJoin(
        right: ParquetIterable[RowParquetRecord],
        onLeft: ColumnPath,
        onRight: ColumnPath
    ): ParquetIterable[RowParquetRecord] = Join.full(iterable, right, onLeft, onRight)

  }
}

/** Allows to iterate over Parquet file(s). Remember to call `close()` when you are done.
  *
  * @tparam T
  *   type that represents schema of Parquet file
  */
trait ParquetIterable[T] extends Iterable[T] with Closeable {

  /** Returns min value of underlying dataset at given path
    *
    * @param columnPath
    *   [[ColumnPath]]
    * @tparam V
    *   type of data at given path
    * @return
    *   min value or [[scala.None]] if there is no matching data or path is invalid
    */
  def min[V: Ordering: ValueDecoder](columnPath: ColumnPath): Option[V] = stats.min(columnPath)

  /** Returns max value of underlying dataset at given path
    *
    * @param columnPath
    *   [[ColumnPath]]
    * @tparam V
    *   type of data at given path
    * @return
    *   max value or [[scala.None]] if there is no matching data or path is invalid
    */
  def max[V: Ordering: ValueDecoder](columnPath: ColumnPath): Option[V] = stats.max(columnPath)

  // TODO docs
  override def size: Int = stats.recordCount.toInt

  // TODO docs
  @experimental
  def concat(other: ParquetIterable[T]): ParquetIterable[T] = new CompoundParquetIterable(Seq(this, other))

  private[parquet4s] def appendTransformation(
      transformation: RowParquetRecord => Iterable[RowParquetRecord]
  ): ParquetIterable[T]

  private[parquet4s] def changeDecoder[U: ParquetRecordDecoder]: ParquetIterable[U]

  private[parquet4s] def stats: Stats

  private[parquet4s] def valueCodecConfiguration: ValueCodecConfiguration

}

private[parquet4s] class ParquetIterableImpl[T](
    iteratorFactory: () => Iterator[RowParquetRecord] & Closeable,
    override val valueCodecConfiguration: ValueCodecConfiguration,
    override val stats: Stats,
    transformations: Seq[RowParquetRecord => Iterable[RowParquetRecord]],
    decode: RowParquetRecord => T
) extends ParquetIterable[T] {

  private var openCloseables: Set[Closeable] = Set.empty

  override def iterator: Iterator[T] = {
    val iterator = iteratorFactory()
    this.synchronized { openCloseables = openCloseables + iterator }

    if (transformations.isEmpty) iterator.map(decode)
    else
      iterator.flatMap(record =>
        transformations
          .foldLeft(Seq(record)) { case (iterables, transformation) =>
            iterables.flatMap(transformation)
          }
          .map(decode)
      )
  }

  override def close(): Unit =
    openCloseables.synchronized {
      openCloseables.foreach(_.close())
      openCloseables = Set.empty
    }

  override private[parquet4s] def appendTransformation(transformation: RowParquetRecord => Iterable[RowParquetRecord]) =
    new ParquetIterableImpl[T](
      iteratorFactory         = iteratorFactory,
      valueCodecConfiguration = valueCodecConfiguration,
      stats                   = stats,
      transformations         = transformations :+ transformation,
      decode                  = decode
    )

  override def changeDecoder[U: ParquetRecordDecoder]: ParquetIterable[U] =
    new ParquetIterableImpl[U](
      iteratorFactory         = iteratorFactory,
      valueCodecConfiguration = valueCodecConfiguration,
      stats                   = stats,
      transformations         = transformations,
      decode = (record: RowParquetRecord) => ParquetRecordDecoder.decode(record, valueCodecConfiguration)
    )

}

private class CompoundParquetIterable[T](components: Seq[ParquetIterable[T]]) extends ParquetIterable[T] {

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

private[parquet4s] class InMemoryParquetIterable[T](
    data: Iterable[RowParquetRecord],
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

  override def iterator: Iterator[T] = data.iterator.flatMap(record =>
    transformations
      .foldLeft(Iterable(record)) { case (iterables, transformation) =>
        iterables.flatMap(transformation)
      }
      .map(decode)
  )
}
