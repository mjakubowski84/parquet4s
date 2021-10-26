package com.github.mjakubowski84.parquet4s

import org.apache.parquet.column.statistics.*
import org.apache.parquet.schema.MessageType

/** Utilises statistics of Parquet files to provide number of records and minimum and maximum value of columns. Values
  * are provided for both unfiltered and filtered reads. Reading statistics from unfiltered files is usually faster as
  * then only file metadata are used. For filtered files certain blocks must be scanned in order to provide correct
  * results.
  */
trait Stats {

  /** @return
    *   Number for records in given path. Filter is considered during calculation.
    */
  def recordCount: Long

  /** @param columnPath
    *   [[ColumnPath]]
    * @param decoder
    *   [[ValueDecoder]] required to decode the value
    * @param ordering
    *   required to sort filtered values
    * @tparam V
    *   type of value stored in the column
    * @return
    *   Minimum value across Parquet data. [[Filter]] is considered during calculation.
    */
  def min[V](columnPath: ColumnPath)(implicit decoder: ValueDecoder[V], ordering: Ordering[V]): Option[V] =
    min(columnPath, None)

  /** @param columnPath
    *   [[ColumnPath]]
    * @param decoder
    *   [[ValueDecoder]] required to decode the value
    * @param ordering
    *   required to sort filtered values
    * @tparam V
    *   type of value stored in the column
    * @return
    *   Maximum value across Parquet data. [[Filter]] is considered during calculation.
    */
  def max[V](columnPath: ColumnPath)(implicit decoder: ValueDecoder[V], ordering: Ordering[V]): Option[V] =
    max(columnPath, None)

  protected[parquet4s] def min[V](columnPath: ColumnPath, currentMin: Option[V])(implicit
      decoder: ValueDecoder[V],
      ordering: Ordering[V]
  ): Option[V]
  protected[parquet4s] def max[V](columnPath: ColumnPath, currentMax: Option[V])(implicit
      decoder: ValueDecoder[V],
      ordering: Ordering[V]
  ): Option[V]

  protected def statsMinValue(statistics: Statistics[?]): Option[Value] =
    statistics match {
      case s if s.isEmpty       => Option.empty[Value]
      case s: IntStatistics     => Option(IntValue(s.genericGetMin))
      case s: LongStatistics    => Option(LongValue(s.genericGetMin))
      case s: BooleanStatistics => Option(BooleanValue(s.genericGetMin))
      case s: BinaryStatistics  => Option(BinaryValue(s.genericGetMin))
      case s: DoubleStatistics  => Option(DoubleValue(s.genericGetMin))
      case s: FloatStatistics   => Option(FloatValue(s.genericGetMin))
    }

  protected def statsMaxValue(statistics: Statistics[?]): Option[Value] =
    statistics match {
      case s if s.isEmpty       => Option.empty[Value]
      case s: IntStatistics     => Option(IntValue(s.genericGetMax))
      case s: LongStatistics    => Option(LongValue(s.genericGetMax))
      case s: BooleanStatistics => Option(BooleanValue(s.genericGetMax))
      case s: BinaryStatistics  => Option(BinaryValue(s.genericGetMax))
      case s: DoubleStatistics  => Option(DoubleValue(s.genericGetMax))
      case s: FloatStatistics   => Option(FloatValue(s.genericGetMax))
    }

}

object Stats {

  /** @param path
    *   [[Path]] to Parquet files
    * @param options
    *   [[ParquetReader.Options]]
    * @param filter
    *   optional [[Filter]] that is considered during calculation of [[Stats]]
    * @return
    *   [[Stats]] of Parquet files
    */
  def apply(
      path: Path,
      options: ParquetReader.Options = ParquetReader.Options(),
      filter: Filter                 = Filter.noopFilter
  ): Stats =
    this.apply(path = path, options = options, projectionSchemaOpt = None, filter = filter)

  /** If you are not interested in Stats of all columns then consider using projection to make the operation faster. If
    * you are going to use a filter mind that your projection has to contain columns that filter refers to.
    *
    * @param path
    *   [[Path]] to Parquet files
    * @param options
    *   [[ParquetReader.Options]]
    * @param filter
    *   optional [[Filter]] that is considered during calculation of [[Stats]]
    * @tparam T
    *   projected type
    * @return
    *   [[Stats]] of Parquet files
    */
  def withProjection[T: ParquetSchemaResolver](
      path: Path,
      options: ParquetReader.Options = ParquetReader.Options(),
      filter: Filter                 = Filter.noopFilter
  ): Stats =
    this.apply(
      path                = path,
      options             = options,
      projectionSchemaOpt = Option(ParquetSchemaResolver.resolveSchema[T]),
      filter              = filter
    )

  private[parquet4s] def apply(
      path: Path,
      options: ParquetReader.Options,
      projectionSchemaOpt: Option[MessageType],
      filter: Filter
  ): Stats = new LazyDelegateStats(path, options, projectionSchemaOpt, filter)

}

/** Calculates [[Stats]] from multiple files.
  */
private class CompoundStats(statsSeq: Seq[Stats]) extends Stats {
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

private class LazyDelegateStats(
    path: Path,
    options: ParquetReader.Options,
    projectionSchemaOpt: Option[MessageType],
    filter: Filter
) extends Stats {
  private lazy val delegate: Stats = {
    val fs = path.toHadoop.getFileSystem(options.hadoopConf)
    val statsArray = fs.listStatus(path.toHadoop).map {
      case status if filter == Filter.noopFilter => new FileStats(status, options, projectionSchemaOpt)
      case status                                => new FilteredFileStats(status, options, projectionSchemaOpt, filter)
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
