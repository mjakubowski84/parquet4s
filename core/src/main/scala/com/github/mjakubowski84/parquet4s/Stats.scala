package com.github.mjakubowski84.parquet4s

import com.github.mjakubowski84.parquet4s.stats.LazyDelegateStats
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
    min[V](columnPath, None)

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
    max[V](columnPath, None)

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

  /** Builds an instance of [[Stats]].
    */
  trait Builder {

    /** @param options
      *   configuration of how Parquet files should be read
      */
    def options(options: ParquetReader.Options): Builder

    /** @param filter
      *   optional before-read filter; no filtering is applied by default; check [[Filter]] for more details
      */
    def filter(filter: Filter): Builder

    /** If you are not interested in Stats of all columns then consider using projection to make the operation faster.
      * If you are going to use a filter mind that your projection has to contain columns that filter refers to
      * @tparam T
      *   projection schema
      */
    def projection[T: ParquetSchemaResolver]: Builder

    /** @param path
      *   [[Path]] to Parquet files, e.g.: {{{Path("file:///data/users")}}}
      * @return
      *   final [[Stats]]
      */
    def stats(path: Path): Stats
  }

  private case class BuilderImpl(
      options: ParquetReader.Options           = ParquetReader.Options(),
      filter: Filter                           = Filter.noopFilter,
      projectionSchemaOpt: Option[MessageType] = None
  ) extends Builder {
    override def options(options: ParquetReader.Options): Builder = this.copy(options = options)
    override def filter(filter: Filter): Builder                  = this.copy(filter = filter)
    override def projection[T: ParquetSchemaResolver]: Builder =
      this.copy(projectionSchemaOpt = Option(ParquetSchemaResolver.resolveSchema[T]))
    override def stats(path: Path): Stats =
      apply(path = path, options = options, projectionSchemaOpt = projectionSchemaOpt, filter = filter)
  }

  private[parquet4s] def apply(
      path: Path,
      options: ParquetReader.Options,
      projectionSchemaOpt: Option[MessageType],
      filter: Filter
  ): Stats = new LazyDelegateStats(path, options, projectionSchemaOpt, filter)

  def builder: Builder = BuilderImpl()

}
