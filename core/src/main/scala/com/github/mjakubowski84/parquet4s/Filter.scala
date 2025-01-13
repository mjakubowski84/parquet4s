package com.github.mjakubowski84.parquet4s

import org.apache.parquet.filter.RecordFilter as ParquetRecordFilter
import org.apache.parquet.filter.UnboundRecordFilter
import org.apache.parquet.filter2.compat.FilterCompat
import org.apache.parquet.filter2.predicate.Operators.*
import org.apache.parquet.filter2.predicate.{FilterApi, FilterPredicate, Statistics, UserDefinedPredicate}
import org.apache.parquet.io.api.Binary
import scala.annotation.nowarn

/** Provides filtering of Parquet records based on their index. Cannot be joined with other filters using boolean
  * algebra.
  */
@experimental
trait RecordFilter extends Filter {

  protected[parquet4s] def toUnboundedRecordFilter: UnboundRecordFilter

  override protected[parquet4s] def toFilterCompat(
      valueCodecConfiguration: ValueCodecConfiguration
  ): FilterCompat.Filter =
    FilterCompat.get(toUnboundedRecordFilter)

  override private[parquet4s] def toNonPredicateFilterCompat: FilterCompat.Filter =
    FilterCompat.get(toUnboundedRecordFilter)

}

object RecordFilter {

  /** Creates a filter which drops Parquet records that do not match given `indexFilter`.
    * @param indexFilter
    *   to be applied to each record index
    */
  @experimental
  def apply(indexFilter: Long => Boolean): RecordFilter = new RecordFilterImpl(indexFilter)
}

private class RecordFilterImpl(indexFilter: Long => Boolean) extends RecordFilter {

  override protected[parquet4s] def toPredicate(valueCodecConfiguration: ValueCodecConfiguration): FilterPredicate =
    throw new UnsupportedOperationException("RecordFilter cannot be joined with predicates")

  override protected[parquet4s] def toUnboundedRecordFilter: UnboundRecordFilter = _ =>
    new ParquetRecordFilter {
      private var counter: Long = 0L
      override def isMatch: Boolean = {
        val currentIndex = counter
        counter = counter + 1
        indexFilter(currentIndex)
      }
    }
}

/** Filter provides a way to define filtering predicates with a simple algebra. Use filters to process your files while
  * it is read from a file system and BEFORE its content is transferred to your application. <br/> You can filter by
  * values of leaf fields of your schema. Check
  * [[https://github.com/mjakubowski84/parquet4s/blob/master/supportedTypes.md here]] which field types are supported.
  * Refer to fields/columns using case class [[Col]]. Define filtering conditions using simple algebraic operators, like
  * equality or greater then (check [[Col]]'s fields. Combine filter by means of simple algebraic operators `&&`, `||`
  * and `!`. <br/> You can also define filters for partitions. Keep in mind that partition value can be only a String.
  *
  * @example
  *   Given schema: {{{case class User(id: Long, age: Int, gender: String)}}} Search for males in age below 40
  *   (exclusive) or above 50 (inclusive) {{{Col("gender") === "male" && (Col("age") < 40 || Col("age") >= 50)}}}
  */
trait Filter {

  protected[parquet4s] def toPredicate(valueCodecConfiguration: ValueCodecConfiguration): FilterPredicate

  protected[parquet4s] def toFilterCompat(valueCodecConfiguration: ValueCodecConfiguration): FilterCompat.Filter =
    FilterCompat.get(toPredicate(valueCodecConfiguration))

  private[parquet4s] def toNonPredicateFilterCompat: FilterCompat.Filter = FilterCompat.NOOP

  /** @return
    *   New filter that passes data that match `this` <b>and<b> `other` filter.
    */
  def &&(other: Filter): Filter = Filter.andFilter(this, other)

  /** @return
    *   New filter that passes data that match `this` <b>or<b> `other` filter.
    */
  def ||(other: Filter): Filter = Filter.orFilter(this, other)

  /** @return
    *   Returns new filter that reverts `this`
    */
  def unary_! : Filter = Filter.notFilter(this)

}

trait FilterOps {

  this: ColumnPath =>

  /** @return
    *   Returns [[Filter]] that passes data that, in `this` column, is <b>equal to</b> provided value
    */
  def ===[In, V <: Comparable[V], C <: Column[V] & SupportsEqNotEq](in: In)(implicit
      encoder: FilterEncoder[In, V, C]
  ): Filter =
    Filter.eqFilter(this, in)(encoder)

  /** @return
    *   Returns [[Filter]] that passes data that, in `this` column, is <b>not equal to</b> provided value
    */
  def !==[In, V <: Comparable[V], C <: Column[V] & SupportsEqNotEq](in: In)(implicit
      encoder: FilterEncoder[In, V, C]
  ): Filter =
    Filter.neqFilter(this, in)(encoder)

  /** @return
    *   Returns [[Filter]] that passes data that, in `this` column, is <b>greater than</b> provided value
    */
  def >[In, V <: Comparable[V], C <: Column[V] & SupportsLtGt](in: In)(implicit
      encoder: FilterEncoder[In, V, C]
  ): Filter =
    Filter.gtFilter(this, in)(encoder)

  /** @return
    *   Returns [[Filter]] that passes data that, in `this` column, is <b>greater than or equal to</b> provided value
    */
  def >=[In, V <: Comparable[V], C <: Column[V] & SupportsLtGt](in: In)(implicit
      encoder: FilterEncoder[In, V, C]
  ): Filter =
    Filter.gtEqFilter(this, in)(encoder)

  /** @return
    *   Returns [[Filter]] that passes data that, in `this` column, is <b>less than</b> provided value
    */
  def <[In, V <: Comparable[V], C <: Column[V] & SupportsLtGt](in: In)(implicit
      encoder: FilterEncoder[In, V, C]
  ): Filter =
    Filter.ltFilter(this, in)(encoder)

  /** @return
    *   Returns [[Filter]] that passes data that, in `this` column, is <b>less than or equal to</b> provided value
    */
  def <=[In, V <: Comparable[V], C <: Column[V] & SupportsLtGt](in: In)(implicit
      encoder: FilterEncoder[In, V, C]
  ): Filter =
    Filter.ltEqFilter(this, in)(encoder)

  /** @return
    *   Returns [[Filter]] that passes data that, in `this` column, is <b>equal to</b> one of the provided values
    */
  def in[In, V <: Comparable[V], C <: Column[V] & SupportsEqNotEq](in: In, inx: In*)(implicit
      encoder: FilterEncoder[In, V, C]
  ): Filter =
    Filter.inFilter(this, Set(in) ++ inx.toSet)(encoder)

  /** @return
    *   Returns [[Filter]] that passes data that, in `this` column, is <b>equal to</b> one of the provided values
    */
  def in[In, V <: Comparable[V], C <: Column[V] & SupportsEqNotEq](in: Iterable[In])(implicit
      encoder: FilterEncoder[In, V, C]
  ): Filter =
    Filter.inFilter(this, in)(encoder)

  /** @return
    *   Returns [[Filter]] that passes data that, in `this` column, satisfy provided [[UDP]] predicate.
    */
  def udp[V <: Comparable[V], C <: Column[V]](udp: UDP[V])(implicit
      ordering: Ordering[V],
      columnFactory: ColumnFactory[V, C]
  ): Filter =
    Filter.udpFilter[V, C](this, udp)

  /** @return
    *   Returns [[Filter]] that passes data that, in `this` column, is <b>null</b>
    */
  def isNull[In](implicit encoder: FilterEncoder[In, ?, ?]): Filter =
    Filter.isNullFilter(this)

  /** @return
    *   Returns [[Filter]] that passes data that, in `this` column, is <b>not null</b>
    */
  def isNotNull[In](implicit encoder: FilterEncoder[In, ?, ?]): Filter =
    Filter.isNotNullFilter(this)

}

object Filter {

  def eqFilter[In, V <: Comparable[V], C <: Column[V] & SupportsEqNotEq](columnPath: ColumnPath, in: In)(implicit
      encoder: FilterEncoder[In, V, C]
  ): Filter =
    new Filter {
      def toPredicate(valueCodecConfiguration: ValueCodecConfiguration): FilterPredicate =
        FilterApi.eq(encoder.columnFactory(columnPath), encoder.encode(in, valueCodecConfiguration))
    }

  def neqFilter[In, V <: Comparable[V], C <: Column[V] & SupportsEqNotEq](columnPath: ColumnPath, in: In)(implicit
      encoder: FilterEncoder[In, V, C]
  ): Filter =
    new Filter {
      def toPredicate(valueCodecConfiguration: ValueCodecConfiguration): FilterPredicate =
        FilterApi.notEq(encoder.columnFactory(columnPath), encoder.encode(in, valueCodecConfiguration))
    }

  def andFilter(left: Filter, right: Filter): Filter = new Filter {
    def toPredicate(valueCodecConfiguration: ValueCodecConfiguration): FilterPredicate =
      FilterApi.and(left.toPredicate(valueCodecConfiguration), right.toPredicate(valueCodecConfiguration))
  }

  def orFilter(left: Filter, right: Filter): Filter = new Filter {
    def toPredicate(valueCodecConfiguration: ValueCodecConfiguration): FilterPredicate =
      FilterApi.or(left.toPredicate(valueCodecConfiguration), right.toPredicate(valueCodecConfiguration))
  }

  def notFilter(filter: Filter): Filter = new Filter {
    def toPredicate(valueCodecConfiguration: ValueCodecConfiguration): FilterPredicate =
      FilterApi.not(filter.toPredicate(valueCodecConfiguration))
  }

  def gtFilter[In, V <: Comparable[V], C <: Column[V] & SupportsLtGt](columnPath: ColumnPath, in: In)(implicit
      encoder: FilterEncoder[In, V, C]
  ): Filter =
    new Filter {
      def toPredicate(valueCodecConfiguration: ValueCodecConfiguration): FilterPredicate =
        FilterApi.gt(encoder.columnFactory(columnPath), encoder.encode(in, valueCodecConfiguration))
    }

  def gtEqFilter[In, V <: Comparable[V], C <: Column[V] & SupportsLtGt](columnPath: ColumnPath, in: In)(implicit
      encoder: FilterEncoder[In, V, C]
  ): Filter =
    new Filter {
      def toPredicate(valueCodecConfiguration: ValueCodecConfiguration): FilterPredicate =
        FilterApi.gtEq(encoder.columnFactory(columnPath), encoder.encode(in, valueCodecConfiguration))
    }

  def ltFilter[In, V <: Comparable[V], C <: Column[V] & SupportsLtGt](columnPath: ColumnPath, in: In)(implicit
      encoder: FilterEncoder[In, V, C]
  ): Filter =
    new Filter {
      def toPredicate(valueCodecConfiguration: ValueCodecConfiguration): FilterPredicate =
        FilterApi.lt(encoder.columnFactory(columnPath), encoder.encode(in, valueCodecConfiguration))
    }

  def ltEqFilter[In, V <: Comparable[V], C <: Column[V] & SupportsLtGt](columnPath: ColumnPath, in: In)(implicit
      encoder: FilterEncoder[In, V, C]
  ): Filter =
    new Filter {
      def toPredicate(valueCodecConfiguration: ValueCodecConfiguration): FilterPredicate =
        FilterApi.ltEq(encoder.columnFactory(columnPath), encoder.encode(in, valueCodecConfiguration))
    }

  def inFilter[In, V <: Comparable[V], C <: Column[V] & SupportsEqNotEq](columnPath: ColumnPath, in: Iterable[In])(
      implicit encoder: FilterEncoder[In, V, C]
  ): Filter =
    new Filter {
      require(in.nonEmpty, "Cannot filter with an empty list of keys.")
      override def toPredicate(valueCodecConfiguration: ValueCodecConfiguration): FilterPredicate = {
        val filterValues = in.map(encoder.encode(_, valueCodecConfiguration)).toSet
        FilterApi.userDefined(encoder.columnFactory(columnPath), new InPredicate(filterValues))
      }
    }

  def udpFilter[V <: Comparable[V], C <: Column[V]](columnPath: ColumnPath, udp: UDP[V])(implicit
      ordering: Ordering[V],
      columnFactory: ColumnFactory[V, C]
  ): Filter =
    new Filter {
      def toPredicate(valueCodecConfiguration: ValueCodecConfiguration): FilterPredicate =
        FilterApi.userDefined(columnFactory(columnPath), new UDPAdapter[V](udp))
    }

  val noopFilter: Filter = new Filter {
    override def toPredicate(valueCodecConfiguration: ValueCodecConfiguration): FilterPredicate = new FilterPredicate {
      override def accept[R](visitor: FilterPredicate.Visitor[R]): R =
        throw new UnsupportedOperationException
    }

    override def toFilterCompat(valueCodecConfiguration: ValueCodecConfiguration): FilterCompat.Filter =
      FilterCompat.NOOP
  }

  def isNullFilter[In](columnPath: ColumnPath)(implicit encoder: FilterEncoder[In, ?, ?]): Filter =
    new Filter {
      def toPredicate(valueCodecConfiguration: ValueCodecConfiguration): FilterPredicate =
        new Eq[encoder.columnFactory.Value](
          encoder.columnFactory(columnPath).asInstanceOf[Column[encoder.columnFactory.Value]],
          null.asInstanceOf[encoder.columnFactory.Value]
        )
    }

  def isNotNullFilter[In](columnPath: ColumnPath)(implicit encoder: FilterEncoder[In, ?, ?]): Filter =
    notFilter(isNullFilter(columnPath))

}

/** Constructs instance of [[org.apache.parquet.filter2.predicate.Operators.Column]] af given column path and type.
  */
trait ColumnFactory[V <: Comparable[V], C <: Column[V]] {

  private[parquet4s] type Value  = V
  private[parquet4s] type Column = C

  def apply(columnPath: ColumnPath): C
}

object ColumnFactory {

  @inline
  private def apply[V <: Comparable[V], C <: Column[V]](f: String => C): ColumnFactory[V, C] =
    columnPath => f(columnPath.toString)

  implicit val intColumnFactory: ColumnFactory[java.lang.Integer, IntColumn]         = apply(FilterApi.intColumn)
  implicit val longColumnFactory: ColumnFactory[java.lang.Long, LongColumn]          = apply(FilterApi.longColumn)
  implicit val floatColumnFactory: ColumnFactory[java.lang.Float, FloatColumn]       = apply(FilterApi.floatColumn)
  implicit val doubleColumnFactory: ColumnFactory[java.lang.Double, DoubleColumn]    = apply(FilterApi.doubleColumn)
  implicit val booleanColumnFactory: ColumnFactory[java.lang.Boolean, BooleanColumn] = apply(FilterApi.booleanColumn)
  implicit val binaryColumnFactory: ColumnFactory[Binary, BinaryColumn]              = apply(FilterApi.binaryColumn)
}

// TODO docs
trait FilterEncoder[-In, V <: Comparable[V], C <: Column[V]] {

  val columnFactory: ColumnFactory[V, C]

  /** Encodes user type to internal Parquet type.
    */
  val encode: (In, ValueCodecConfiguration) => V
}

private class FilterEncoderImpl[-In, V <: Comparable[V], C <: Column[V]](
    val encode: (In, ValueCodecConfiguration) => V,
    val columnFactory: ColumnFactory[V, C]
) extends FilterEncoder[In, V, C]

object FilterEncoder {

  def apply[In, V <: Comparable[V], C <: Column[V]](encode: (In, ValueCodecConfiguration) => V)(implicit
      columnFactory: ColumnFactory[V, C]
  ): FilterEncoder[In, V, C] =
    new FilterEncoderImpl(encode, columnFactory)

  implicit val booleanEncoder: FilterEncoder[Boolean, java.lang.Boolean, BooleanColumn] =
    apply[Boolean, java.lang.Boolean, BooleanColumn]((v, _) => v)
  implicit val intEncoder: FilterEncoder[Int, java.lang.Integer, IntColumn] =
    apply[Int, java.lang.Integer, IntColumn]((v, _) => v)
  implicit val longEncoder: FilterEncoder[Long, java.lang.Long, LongColumn] =
    apply[Long, java.lang.Long, LongColumn]((v, _) => v)
  implicit val floatEncoder: FilterEncoder[Float, java.lang.Float, FloatColumn] =
    apply[Float, java.lang.Float, FloatColumn]((v, _) => v)
  implicit val doubleEncoder: FilterEncoder[Double, java.lang.Double, DoubleColumn] =
    apply[Double, java.lang.Double, DoubleColumn]((v, _) => v)
  implicit val shortEncoder: FilterEncoder[Short, java.lang.Integer, IntColumn] =
    apply[Short, java.lang.Integer, IntColumn]((v, _) => v.toInt)
  implicit val byteEncoder: FilterEncoder[Byte, java.lang.Integer, IntColumn] =
    apply[Byte, java.lang.Integer, IntColumn]((v, _) => v.toInt)
  implicit val charEncoder: FilterEncoder[Char, java.lang.Integer, IntColumn] =
    apply[Char, java.lang.Integer, IntColumn]((v, _) => v.toInt)
  implicit val byteArrayEncoder: FilterEncoder[Array[Byte], Binary, BinaryColumn] =
    apply[Array[Byte], Binary, BinaryColumn]((v, _) => Binary.fromReusedByteArray(v))
  implicit val stringEncoder: FilterEncoder[String, Binary, BinaryColumn] =
    apply[String, Binary, BinaryColumn]((v, _) => Binary.fromString(v))
  implicit val sqlDateEncoder: FilterEncoder[java.sql.Date, java.lang.Integer, IntColumn] =
    apply[java.sql.Date, java.lang.Integer, IntColumn](
      ValueEncoder.sqlDateEncoder.encode(_, _).asInstanceOf[PrimitiveValue[Int]].value
    )
  implicit val localDateEncoder: FilterEncoder[java.time.LocalDate, java.lang.Integer, IntColumn] =
    apply[java.time.LocalDate, java.lang.Integer, IntColumn](
      ValueEncoder.localDateEncoder.encode(_, _).asInstanceOf[PrimitiveValue[Int]].value
    )
  implicit val decimalEncoder: FilterEncoder[BigDecimal, Binary, BinaryColumn] =
    DecimalFormat.Default.Implicits.decimalFilterEncoder

}

@deprecated(message = "No longer in use by Parquet4s", since = "2.21.0")
trait FilterDecoder[+In, -V] {

  /** Decodes user type from internal Parquet type.
    */
  val decode: (V, ValueCodecConfiguration) => In
}

@nowarn
private class FilterDecoderImpl[+In, -V](val decode: (V, ValueCodecConfiguration) => In) extends FilterDecoder[In, V]

@nowarn
object FilterDecoder {

  @deprecated(message = "No longer in use by Parquet4s", since = "2.21.0")
  def apply[In, V](decode: (V, ValueCodecConfiguration) => In): FilterDecoder[In, V] = new FilterDecoderImpl(decode)

}

@deprecated(message = "No longer in use by Parquet4s", since = "2.21.0")
trait FilterCodec[In, V <: Comparable[V], C <: Column[V]] extends FilterEncoder[In, V, C] with FilterDecoder[In, V]

@nowarn
private class FilterCodecImpl[In, V <: Comparable[V], C <: Column[V]](
    override val encode: (In, ValueCodecConfiguration) => V,
    override val decode: (V, ValueCodecConfiguration)  => In,
    override val columnFactory: ColumnFactory[V, C]
) extends FilterCodec[In, V, C]

@nowarn
object FilterCodec {

  @deprecated(message = "No longer in use by Parquet4s", since = "2.21.0")
  def apply[In, V <: Comparable[V], C <: Column[V]](
      encode: (In, ValueCodecConfiguration) => V,
      decode: (V, ValueCodecConfiguration)  => In
  )(implicit columnFactory: ColumnFactory[V, C]): FilterCodec[In, V, C] =
    new FilterCodecImpl(encode, decode, columnFactory)

  implicit def codec[In, V <: Comparable[V], C <: Column[V]](implicit
      encoder: FilterEncoder[In, V, C],
      decoder: FilterDecoder[In, V]
  ): FilterCodec[In, V, C] =
    apply(encoder.encode, decoder.decode)(encoder.columnFactory)

}

/** Parquet library has its own implementation of `in` and `notIn` since version 1.12.0. However, still, even in version
  * 1.13.0 it has a bug - notIn and !in gives incorrect results. This custom implementation maybe is not optimal but
  * returns correct results.
  */
private class InPredicate[T <: Comparable[T]](values: Set[T]) extends UserDefinedPredicate[T] with Serializable {
  override def keep(value: T): Boolean = values.contains(value)

  override def canDrop(statistics: Statistics[T]): Boolean = {
    val compare   = statistics.getComparator.compare(_, _)
    val min       = statistics.getMin
    val max       = statistics.getMax
    val isInRange = (value: T) => compare(value, min) >= 0 && compare(value, max) <= 0
    !values.exists(isInRange)
  }

  override def inverseCanDrop(statistics: Statistics[T]): Boolean = false

  override def toString: String = values.mkString("in(", ", ", ")")
}
