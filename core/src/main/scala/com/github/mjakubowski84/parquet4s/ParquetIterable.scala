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

    /** Creates a new instance of [[ParquetIterable]] with a new [[ParquetRecordDecoder]] applied. Especially useful for
      * decoding whole dataset of [[RowParquetRecord]] to the desired class.
      * @tparam U
      *   a type that the dataset shall be decoded to
      */
    def as[U: ParquetRecordDecoder]: ParquetIterable[U] = iterable.changeDecoder[U]

    /** Joins this dataset with `right` dataset using left join. Joins records from this dataset and `right` one that
      * have `onLeft` column and `onRight` column equal respectively. <br/> <br/> <b>Take note</b> that, while this
      * dataset is going to be iterated over, the whole `right` dataset is going to be loaded into memory before.
      * Therefore, it is recommended to run this operation only if the `right` dataset is small enough to fit into
      * memory. You may consider to use `rightJoin` and swap the datasets - if this dataset is actually smaller than
      * `right` one.
      * @param right
      *   the dataset to join with
      * @param onLeft
      *   join column in this dataset
      * @param onRight
      *   join column in `right` dataset
      * @return
      *   [[ParquetIterable]] with joined records
      */
    @experimental
    def leftJoin(
        right: ParquetIterable[RowParquetRecord],
        onLeft: ColumnPath,
        onRight: ColumnPath
    ): ParquetIterable[RowParquetRecord] = Join.left(iterable, right, onLeft, onRight)

    /** Joins this dataset with `right` dataset using right join. Joins records from this dataset and `right` one that
      * have `onLeft` column and `onRight` column equal respectively. <br/> <br/> <b>Take note</b> that, while this
      * dataset is going to be iterated over, the whole `right` dataset is going to be loaded into memory before.
      * Therefore, it is recommended to run this operation only if the `right` dataset is small enough to fit into
      * memory. You may consider to use `leftJoin` and swap the datasets - if this dataset is actually smaller than
      * `right` one.
      * @param right
      *   the dataset to join with
      * @param onLeft
      *   join column in this dataset
      * @param onRight
      *   join column in `right` dataset
      * @return
      *   [[ParquetIterable]] with joined records
      */
    @experimental
    def rightJoin(
        right: ParquetIterable[RowParquetRecord],
        onLeft: ColumnPath,
        onRight: ColumnPath
    ): ParquetIterable[RowParquetRecord] = Join.right(iterable, right, onLeft, onRight)

    /** Joins this dataset with `right` dataset using inner join. Joins records from this dataset and `right` one that
      * have `onLeft` column and `onRight` column equal respectively. <br/> <br/> <b>Take note</b> that, while this
      * dataset is going to be iterated over, the whole `right` dataset is going to be loaded into memory before.
      * Therefore, it is recommended to run this operation only if the `right` dataset is small enough to fit into
      * memory. You may consider to swap the datasets - if this dataset is actually smaller than `right` one.
      * @param right
      *   the dataset to join with
      * @param onLeft
      *   join column in this dataset
      * @param onRight
      *   join column in `right` dataset
      * @return
      *   [[ParquetIterable]] with joined records
      */
    @experimental
    def innerJoin(
        right: ParquetIterable[RowParquetRecord],
        onLeft: ColumnPath,
        onRight: ColumnPath
    ): ParquetIterable[RowParquetRecord] = Join.inner(iterable, right, onLeft, onRight)

    /** Joins this dataset with `right` dataset using full join. Joins records from this dataset and `right` one that
      * have `onLeft` column and `onRight` column equal respectively. <br/> <br/> <b>Take note</b> that, while this
      * dataset is going to be iterated over, the whole `right` dataset is going to be loaded into memory before.
      * Therefore, it is recommended to run this operation only if the `right` dataset is small enough to fit into
      * memory. You may consider to swap the datasets - if this dataset is actually smaller than `right` one.
      * @param right
      *   the dataset to join with
      * @param onLeft
      *   join column in this dataset
      * @param onRight
      *   join column in `right` dataset
      * @return
      *   [[ParquetIterable]] with joined records
      */
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

  /** Returns the size of the underlying dataset. Filter is considered during calculation. Tries to leverage Parquet
    * metadata and statistics in order to avoid full traverse over the dataset.
    */
  override def size: Int = stats.recordCount.toInt

  /** Appends `other` dataset to this one. `Other` is not read until this dataset is not fully iterated over.
    * @param other
    *   dataset to be concatenated with this
    */
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
