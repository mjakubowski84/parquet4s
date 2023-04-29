package com.github.mjakubowski84.parquet4s

import org.apache.parquet.filter2.compat.FilterCompat
import org.apache.parquet.filter2.predicate.Operators.*
import org.apache.parquet.filter2.predicate.{FilterApi, FilterPredicate, Statistics, UserDefinedPredicate}
import org.apache.parquet.io.api.Binary

import scala.language.{existentials, implicitConversions}
import scala.jdk.CollectionConverters.*

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

  private[parquet4s] def toFilterCompat(valueCodecConfiguration: ValueCodecConfiguration): FilterCompat.Filter =
    FilterCompat.get(toPredicate(valueCodecConfiguration))

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
      codec: FilterCodec[In, V, C]
  ): Filter =
    Filter.eqFilter(this, in)

  /** @return
    *   Returns [[Filter]] that passes data that, in `this` column, is <b>not equal to</b> provided value
    */
  def !==[In, V <: Comparable[V], C <: Column[V] & SupportsEqNotEq](in: In)(implicit
      codec: FilterCodec[In, V, C]
  ): Filter =
    Filter.neqFilter(this, in)

  /** @return
    *   Returns [[Filter]] that passes data that, in `this` column, is <b>greater than</b> provided value
    */
  def >[In, V <: Comparable[V], C <: Column[V] & SupportsLtGt](in: In)(implicit
      codec: FilterCodec[In, V, C]
  ): Filter =
    Filter.gtFilter(this, in)

  /** @return
    *   Returns [[Filter]] that passes data that, in `this` column, is <b>greater than or equal to</b> provided value
    */
  def >=[In, V <: Comparable[V], C <: Column[V] & SupportsLtGt](in: In)(implicit
      codec: FilterCodec[In, V, C]
  ): Filter =
    Filter.gtEqFilter(this, in)

  /** @return
    *   Returns [[Filter]] that passes data that, in `this` column, is <b>less than</b> provided value
    */
  def <[In, V <: Comparable[V], C <: Column[V] & SupportsLtGt](in: In)(implicit
      codec: FilterCodec[In, V, C]
  ): Filter =
    Filter.ltFilter(this, in)

  /** @return
    *   Returns [[Filter]] that passes data that, in `this` column, is <b>less than or equal to</b> provided value
    */
  def <=[In, V <: Comparable[V], C <: Column[V] & SupportsLtGt](in: In)(implicit
      codec: FilterCodec[In, V, C]
  ): Filter =
    Filter.ltEqFilter(this, in)

  /** @return
    *   Returns [[Filter]] that passes data that, in `this` column, is <b>equal to</b> one of the provided values
    */
  def in[In, V <: Comparable[V], C <: Column[V] & SupportsEqNotEq](in: In, inx: In*)(implicit
      codec: FilterCodec[In, V, C]
  ): Filter =
    this.in(in :: inx.toList)

  /** @return
    *   Returns [[Filter]] that passes data that, in `this` column, is <b>equal to</b> one of the provided values
    */
  def in[In, V <: Comparable[V], C <: Column[V] & SupportsEqNotEq](in: Iterable[In])(implicit
      codec: FilterCodec[In, V, C]
  ): Filter =
    Filter.inFilter(this, in)

  /** @return
    *   Returns [[Filter]] that passes data that, in `this` column, is <b>not equal to</b> any of the provided values
    */
  def notin[In, V <: Comparable[V], C <: Column[V] & SupportsEqNotEq](in: In, inx: In*)(implicit
      codec: FilterCodec[In, V, C]
  ): Filter =
    Filter.notInFilter(this, Set(in) ++ inx.toSet)

  /** @return
    *   Returns [[Filter]] that passes data that, in `this` column, is <b>not equal to</b> any of the provided values
    */
  def notin[In, V <: Comparable[V], C <: Column[V] & SupportsEqNotEq](in: Iterable[In])(implicit
      codec: FilterCodec[In, V, C]
  ): Filter =
    Filter.notInFilter(this, in)

  /** @return
    *   Returns [[Filter]] that passes data that, in `this` column, satisfy provided [[UDP]] predicate.
    */
  def udp[In, V <: Comparable[V], C <: Column[V]](udp: UDP[In])(implicit
      ordering: Ordering[In],
      codec: FilterCodec[In, V, C]
  ): Filter =
    Filter.udpFilter[In, V, C](this, udp)

}

object Filter {

  def eqFilter[In, V <: Comparable[V], C <: Column[V] & SupportsEqNotEq](columnPath: ColumnPath, in: In)(implicit
      codec: FilterCodec[In, V, C]
  ): Filter =
    new Filter {
      def toPredicate(valueCodecConfiguration: ValueCodecConfiguration): FilterPredicate =
        FilterApi.eq(codec.columnFactory(columnPath), codec.encode(in, valueCodecConfiguration))
    }

  def neqFilter[In, V <: Comparable[V], C <: Column[V] & SupportsEqNotEq](columnPath: ColumnPath, in: In)(implicit
      codec: FilterCodec[In, V, C]
  ): Filter =
    new Filter {
      def toPredicate(valueCodecConfiguration: ValueCodecConfiguration): FilterPredicate =
        FilterApi.notEq(codec.columnFactory(columnPath), codec.encode(in, valueCodecConfiguration))
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
      codec: FilterCodec[In, V, C]
  ): Filter =
    new Filter {
      def toPredicate(valueCodecConfiguration: ValueCodecConfiguration): FilterPredicate =
        FilterApi.gt(codec.columnFactory(columnPath), codec.encode(in, valueCodecConfiguration))
    }

  def gtEqFilter[In, V <: Comparable[V], C <: Column[V] & SupportsLtGt](columnPath: ColumnPath, in: In)(implicit
      codec: FilterCodec[In, V, C]
  ): Filter =
    new Filter {
      def toPredicate(valueCodecConfiguration: ValueCodecConfiguration): FilterPredicate =
        FilterApi.gtEq(codec.columnFactory(columnPath), codec.encode(in, valueCodecConfiguration))
    }

  def ltFilter[In, V <: Comparable[V], C <: Column[V] & SupportsLtGt](columnPath: ColumnPath, in: In)(implicit
      codec: FilterCodec[In, V, C]
  ): Filter =
    new Filter {
      def toPredicate(valueCodecConfiguration: ValueCodecConfiguration): FilterPredicate =
        FilterApi.lt(codec.columnFactory(columnPath), codec.encode(in, valueCodecConfiguration))
    }

  def ltEqFilter[In, V <: Comparable[V], C <: Column[V] & SupportsLtGt](columnPath: ColumnPath, in: In)(implicit
      codec: FilterCodec[In, V, C]
  ): Filter =
    new Filter {
      def toPredicate(valueCodecConfiguration: ValueCodecConfiguration): FilterPredicate =
        FilterApi.ltEq(codec.columnFactory(columnPath), codec.encode(in, valueCodecConfiguration))
    }

  def inFilter[In, V <: Comparable[V], C <: Column[V] & SupportsEqNotEq](columnPath: ColumnPath, in: Iterable[In])(
      implicit codec: FilterCodec[In, V, C]
  ): Filter =
    new Filter {
      require(in.nonEmpty, "Cannot filter with an empty list of keys.")
      override def toPredicate(valueCodecConfiguration: ValueCodecConfiguration): FilterPredicate = {
        val filterValues = in.map(codec.encode(_, valueCodecConfiguration)).toSet
        FilterApi.in(codec.columnFactory(columnPath), filterValues.asJava)
      }
    }

  def notInFilter[In, V <: Comparable[V], C <: Column[V] & SupportsEqNotEq](columnPath: ColumnPath, in: Iterable[In])(
      implicit codec: FilterCodec[In, V, C]
  ): Filter =
    new Filter {
      require(in.nonEmpty, "Cannot filter with an empty list of keys.")

      override def toPredicate(valueCodecConfiguration: ValueCodecConfiguration): FilterPredicate = {
        val filterValues = in.map(codec.encode(_, valueCodecConfiguration)).toSet
        FilterApi.notIn(codec.columnFactory(columnPath), filterValues.asJava)
      }
    }

  def udpFilter[In, V <: Comparable[V], C <: Column[V]](columnPath: ColumnPath, udp: UDP[In])(implicit
      ordering: Ordering[In],
      codec: FilterCodec[In, V, C]
  ): Filter =
    new Filter {
      def toPredicate(valueCodecConfiguration: ValueCodecConfiguration): FilterPredicate =
        FilterApi.userDefined(
          codec.columnFactory(columnPath),
          new UDPAdapter[In, V](udp, codec, valueCodecConfiguration)
        )
    }

  val noopFilter: Filter = new Filter {
    override def toPredicate(valueCodecConfiguration: ValueCodecConfiguration): FilterPredicate = new FilterPredicate {
      override def accept[R](visitor: FilterPredicate.Visitor[R]): R =
        throw new UnsupportedOperationException
    }

    override def toFilterCompat(valueCodecConfiguration: ValueCodecConfiguration): FilterCompat.Filter =
      FilterCompat.NOOP
  }

}

/** Constructs instance of [[org.apache.parquet.filter2.predicate.Operators.Column]] af given column path and type.
  */
trait ColumnFactory[V <: Comparable[V], C <: Column[V]] {
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

trait FilterEncoder[-In, +V] {

  /** Encodes user type to internal Parquet type.
    */
  val encode: (In, ValueCodecConfiguration) => V
}

trait FilterDecoder[+In, -V] {

  /** Decodes user type from internal Parquet type.
    */
  val decode: (V, ValueCodecConfiguration) => In
}

/** Decodes and encodes user type from/to internal Parquet type. Used during filtering.
  * @tparam In
  *   User type
  * @tparam V
  *   Internal Parquet type
  * @tparam C
  *   Column type
  */
trait FilterCodec[In, V <: Comparable[V], C <: Column[V]] extends FilterEncoder[In, V] with FilterDecoder[In, V] {

  def columnFactory: ColumnFactory[V, C]
}

object FilterCodec {

  def apply[In, V <: Comparable[V], C <: Column[V]](
      encode: (In, ValueCodecConfiguration) => V,
      decode: (V, ValueCodecConfiguration)  => In
  )(implicit columnFactory: ColumnFactory[V, C]): FilterCodec[In, V, C] =
    new FilterCodecImpl(encode, decode, columnFactory)

  implicit val booleanCodec: FilterCodec[Boolean, java.lang.Boolean, BooleanColumn] =
    apply[Boolean, java.lang.Boolean, BooleanColumn]((v, _) => v, (v, _) => v)
  implicit val intCodec: FilterCodec[Int, java.lang.Integer, IntColumn] =
    apply[Int, java.lang.Integer, IntColumn]((v, _) => v, (v, _) => v)
  implicit val longCodec: FilterCodec[Long, java.lang.Long, LongColumn] =
    apply[Long, java.lang.Long, LongColumn]((v, _) => v, (v, _) => v)
  implicit val floatCodec: FilterCodec[Float, java.lang.Float, FloatColumn] =
    apply[Float, java.lang.Float, FloatColumn]((v, _) => v, (v, _) => v)
  implicit val doubleCodec: FilterCodec[Double, java.lang.Double, DoubleColumn] =
    apply[Double, java.lang.Double, DoubleColumn]((v, _) => v, (v, _) => v)
  implicit val shortCodec: FilterCodec[Short, java.lang.Integer, IntColumn] =
    apply[Short, java.lang.Integer, IntColumn]((v, _) => v.toInt, (v, _) => v.toShort)
  implicit val byteCodec: FilterCodec[Byte, java.lang.Integer, IntColumn] =
    apply[Byte, java.lang.Integer, IntColumn]((v, _) => v.toInt, (v, _) => v.toByte)
  implicit val charCodec: FilterCodec[Char, java.lang.Integer, IntColumn] =
    apply[Char, java.lang.Integer, IntColumn]((v, _) => v.toInt, (v, _) => v.toChar)
  implicit val byteArrayCodec: FilterCodec[Array[Byte], Binary, BinaryColumn] =
    apply[Array[Byte], Binary, BinaryColumn]((v, _) => Binary.fromReusedByteArray(v), (v, _) => v.getBytes)
  implicit val stringCodec: FilterCodec[String, Binary, BinaryColumn] =
    apply[String, Binary, BinaryColumn]((v, _) => Binary.fromString(v), (v, _) => v.toStringUsingUTF8)
  implicit val sqlDateCodec: FilterCodec[java.sql.Date, java.lang.Integer, IntColumn] =
    apply[java.sql.Date, java.lang.Integer, IntColumn](
      ValueEncoder.sqlDateEncoder.encode(_, _).asInstanceOf[PrimitiveValue[Int]].value,
      (v, vcc) => ValueDecoder.sqlDateDecoder.decode(IntValue(v), vcc)
    )
  implicit val localDateCodec: FilterCodec[java.time.LocalDate, java.lang.Integer, IntColumn] =
    apply[java.time.LocalDate, java.lang.Integer, IntColumn](
      ValueEncoder.localDateEncoder.encode(_, _).asInstanceOf[PrimitiveValue[Int]].value,
      (v, vcc) => ValueDecoder.localDateDecoder.decode(IntValue(v), vcc)
    )
  implicit val decimalCodec: FilterCodec[BigDecimal, Binary, BinaryColumn] =
    apply[BigDecimal, Binary, BinaryColumn](
      (v, _) => Decimals.binaryFromDecimal(v),
      (v, _) => Decimals.decimalFromBinary(v)
    )
}

private class FilterCodecImpl[In, V <: Comparable[V], C <: Column[V]](
    override val encode: (In, ValueCodecConfiguration) => V,
    override val decode: (V, ValueCodecConfiguration)  => In,
    override val columnFactory: ColumnFactory[V, C]
) extends FilterCodec[In, V, C]

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
