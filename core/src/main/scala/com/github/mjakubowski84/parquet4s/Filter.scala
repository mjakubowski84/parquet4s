package com.github.mjakubowski84.parquet4s

import com.github.mjakubowski84.parquet4s.FilterValue.FilterValueFactory
import com.github.mjakubowski84.parquet4s.FilterValueSet.FilterValueSetFactory
import org.apache.parquet.filter2.compat.FilterCompat
import org.apache.parquet.filter2.predicate.Operators._
import org.apache.parquet.filter2.predicate.{FilterApi, FilterPredicate, Statistics, UserDefinedPredicate}
import org.apache.parquet.io.api.Binary

import scala.collection.immutable
import scala.language.{higherKinds, implicitConversions}

/**
  * Filter provides a way to define filtering predicates with a simple algebra. Use filters to process
  * your files while it is read from a file system and BEFORE its content is transferred to your application.
  *
  * You can filter by values of leaf fields of your schema. Check here which field types are supported. TODO link
  * Refer to fields/columns using case class [[Col]]. Define filtering conditions using simple algebraic operators, like
  * equality or greater then (check [[Col]]'s fields. Combine filter by means of simple algebraic operators `&&`, `||`
  * and `!`.
  *
  * @example
  *          Given schema:
  *          {{{ case class User(id: Long, age: Int, gender: String) }}}
  *          Search for males in age below 40 (exclusive) or above 50 (inclusive)
  *          {{{ Col("gender") === "male" && (Col("age") < 40 || Col("age") >= 50) }}}
  */
trait Filter {

  protected[parquet4s] def toPredicate(valueCodecConfiguration: ValueCodecConfiguration): FilterPredicate

  private[parquet4s] def toFilterCompat(valueCodecConfiguration: ValueCodecConfiguration): FilterCompat.Filter =
    FilterCompat.get(toPredicate(valueCodecConfiguration))

  /**
    * @return New filter that passes data that match `this` <b>and<b> `other` filter.
    */
  def &&(other: Filter): Filter = Filter.andFilter(this, other)

  /**
    * @return New filter that passes data that match `this` <b>or<b> `other` filter.
    */
  def ||(other: Filter): Filter = Filter.orFilter(this, other)

  /**
    * @return Returns new filter that reverts `this`
    */
  def unary_! : Filter = Filter.notFilter(this)

}

case class InPredicate[T <: Comparable[T]](values: Set[T]) extends UserDefinedPredicate[T] with Serializable {
  override def keep(value: T): Boolean = values.contains(value)

  override def canDrop(statistics: Statistics[T]): Boolean = !inverseCanDrop(statistics)

  @inline
  override def inverseCanDrop(statistics: Statistics[T]): Boolean = {
    val compare = statistics.getComparator.compare(_, _)
    val min = statistics.getMin
    val max = statistics.getMax
    val isInRange = (value: T) => compare(value, min) >= 0 && compare(value, max) <= 0
    values.exists(isInRange)
  }

  override def toString: String = values.mkString("in(", ", ", ")")
}

object Filter {

  def eqFilter[V <: Comparable[V], C <: Column[V] with SupportsEqNotEq](columnPath: String, filterValueFactory: FilterValueFactory[V, C]): Filter = new Filter {
    override def toPredicate(valueCodecConfiguration: ValueCodecConfiguration): FilterPredicate = {
      val filterValue = filterValueFactory(valueCodecConfiguration)
      FilterApi.eq(filterValue.columnFactory(columnPath), filterValue.value)
    }
  }

  def neqFilter[V <: Comparable[V], C <: Column[V] with SupportsEqNotEq](columnPath: String, filterValueFactory: FilterValueFactory[V, C]): Filter = new Filter {
    override def toPredicate(valueCodecConfiguration: ValueCodecConfiguration): FilterPredicate = {
      val filterValue = filterValueFactory(valueCodecConfiguration)
      FilterApi.notEq(filterValue.columnFactory(columnPath), filterValue.value)
    }
  }

  def andFilter(left: Filter, right: Filter): Filter = new Filter {
    override def toPredicate(valueCodecConfiguration: ValueCodecConfiguration): FilterPredicate =
      FilterApi.and(left.toPredicate(valueCodecConfiguration), right.toPredicate(valueCodecConfiguration))
  }

  def orFilter(left: Filter, right: Filter): Filter = new Filter {
    override def toPredicate(valueCodecConfiguration: ValueCodecConfiguration): FilterPredicate =
      FilterApi.or(left.toPredicate(valueCodecConfiguration), right.toPredicate(valueCodecConfiguration))
  }

  def notFilter(filter: Filter): Filter = new Filter {
    override def toPredicate(valueCodecConfiguration: ValueCodecConfiguration): FilterPredicate =
      FilterApi.not(filter.toPredicate(valueCodecConfiguration))
  }

  def gtFilter[V <: Comparable[V], C <: Column[V] with SupportsLtGt](columnPath: String, filterValueFactory: FilterValueFactory[V, C]): Filter = new Filter {
    override def toPredicate(valueCodecConfiguration: ValueCodecConfiguration): FilterPredicate = {
      val filterValue = filterValueFactory(valueCodecConfiguration)
      FilterApi.gt(filterValue.columnFactory(columnPath), filterValue.value)
    }
  }

  def gtEqFilter[V <: Comparable[V], C <: Column[V] with SupportsLtGt](columnPath: String, filterValueFactory: FilterValueFactory[V, C]): Filter = new Filter {
    override def toPredicate(valueCodecConfiguration: ValueCodecConfiguration): FilterPredicate = {
      val filterValue = filterValueFactory(valueCodecConfiguration)
      FilterApi.gtEq(filterValue.columnFactory(columnPath), filterValue.value)
    }
  }

  def ltFilter[V <: Comparable[V], C <: Column[V] with SupportsLtGt](columnPath: String, filterValueFactory: FilterValueFactory[V, C]): Filter = new Filter {
    override def toPredicate(valueCodecConfiguration: ValueCodecConfiguration): FilterPredicate = {
      val filterValue = filterValueFactory(valueCodecConfiguration)
      FilterApi.lt(filterValue.columnFactory(columnPath), filterValue.value)
    }
  }

  def ltEqFilter[V <: Comparable[V], C <: Column[V] with SupportsLtGt](columnPath: String, filterValueFactory: FilterValueFactory[V, C]): Filter = new Filter {
    override def toPredicate(valueCodecConfiguration: ValueCodecConfiguration): FilterPredicate = {
      val filterValue = filterValueFactory(valueCodecConfiguration)
      FilterApi.ltEq(filterValue.columnFactory(columnPath), filterValue.value)
    }
  }

  def inFilter[V <: Comparable[V], C <: Column[V] with SupportsEqNotEq](columnPath: String, filterValueSetFactory: FilterValueSetFactory[V, C]): Filter =
    new Filter {
      override def toPredicate(valueCodecConfiguration: ValueCodecConfiguration): FilterPredicate = {
        val filterValue = filterValueSetFactory(valueCodecConfiguration)
        FilterApi.userDefined(filterValue.columnFactory(columnPath), InPredicate(filterValue.value))
      }
    }

  val noopFilter: Filter = new Filter {
    override def toPredicate(valueCodecConfiguration: ValueCodecConfiguration): FilterPredicate = new FilterPredicate {
      override def accept[R](visitor: FilterPredicate.Visitor[R]): R = {
        throw new UnsupportedOperationException
      }
    }

    override def toFilterCompat(valueCodecConfiguration: ValueCodecConfiguration): FilterCompat.Filter =
      FilterCompat.NOOP
  }

}

/**
  * Represent a column path that you want to apply a filter against. Use a dot-nation to refer to embedded fields.
  *
  * @example
  *          {{{ Col("user.address.postcode") === "00000" }}}
  */
case class Col(columnPath: String) {

  /**
    * @return Returns [[Filter]] that passes data that, in `this` column, is <b>equal to</b> provided value
    */
  def ===[V <: Comparable[V], C <: Column[V] with SupportsEqNotEq](filterValueFactory: FilterValueFactory[V, C]): Filter =
    Filter.eqFilter(columnPath, filterValueFactory)

  /**
    * @return Returns [[Filter]] that passes data that, in `this` column, is <b>not equal to</b> provided value
    */
  def !==[V <: Comparable[V], C <: Column[V] with SupportsEqNotEq](filterValueFactory: FilterValueFactory[V, C]): Filter =
    Filter.neqFilter(columnPath, filterValueFactory)

  /**
    * @return Returns [[Filter]] that passes data that, in `this` column, is <b>greater than</b> provided value
    */
  def >[V <: Comparable[V], C <: Column[V] with SupportsLtGt](filterValueFactory: FilterValueFactory[V, C]): Filter =
    Filter.gtFilter(columnPath, filterValueFactory)

  /**
    * @return Returns [[Filter]] that passes data that, in `this` column, is <b>greater than or equal to</b> provided value
    */
  def >=[V <: Comparable[V], C <: Column[V] with SupportsLtGt](filterValueFactory: FilterValueFactory[V, C]): Filter =
    Filter.gtEqFilter(columnPath, filterValueFactory)

  /**
    * @return Returns [[Filter]] that passes data that, in `this` column, is <b>less than</b> provided value
    */
  def <[V <: Comparable[V], C <: Column[V] with SupportsLtGt](filterValueFactory: FilterValueFactory[V, C]): Filter =
    Filter.ltFilter(columnPath, filterValueFactory)

  /**
    * @return Returns [[Filter]] that passes data that, in `this` column, is <b>less than or equal to</b> provided value
    */
  def <=[V <: Comparable[V], C <: Column[V] with SupportsLtGt](filterValueFactory: FilterValueFactory[V, C]): Filter =
    Filter.ltEqFilter(columnPath, filterValueFactory)

  /**
    * @return Returns [[Filter]] that passes data that, in `this` column, is <b>equal to</b> one of the provided values
    */
  def in[V <: Comparable[V], C <: Column[V] with SupportsEqNotEq](filterValueSetFactory: FilterValueSetFactory[V, C]): Filter =
    Filter.inFilter(columnPath, filterValueSetFactory)
}

private trait FilterValueConverter[In, V <: Comparable[V], C <: Column[V]] {

  def convert(in: In): FilterValueFactory[V, C]

}

private class SimpleFilterValueConverter[In, V <: Comparable[V], C <: Column[V]](f: In => FilterValue[V, C]
                                                                                ) extends FilterValueConverter[In, V, C] {
  override def convert(in: In): FilterValueFactory[V, C] = _ => f(in)
}

private class BinaryFilterValueConverter[In](codec: ValueCodec[In]) extends FilterValueConverter[In, Binary, BinaryColumn] {
  override def convert(in: In): FilterValueFactory[Binary, BinaryColumn] =
    conf => FilterValue.binary(codec.encode(in, conf).asInstanceOf[PrimitiveValue[Binary]].value)
}

private class IntFilterValueConverter[In](codec: ValueCodec[In]) extends FilterValueConverter[In, Integer, IntColumn] {
  override def convert(in: In): FilterValueFactory[Integer, IntColumn] =
    conf => FilterValue.int(codec.encode(in, conf).asInstanceOf[PrimitiveValue[Int]].value)
}

private object FilterValueConverter {

  implicit val stringFilterValueConverter: FilterValueConverter[String, Binary, BinaryColumn] =
    new BinaryFilterValueConverter(ValueCodec.stringCodec)

  implicit val intFilterValueConverter: FilterValueConverter[Int, Integer, IntColumn] =
    new SimpleFilterValueConverter(FilterValue.int)

  implicit val shortFilterValueConverter: FilterValueConverter[Short, Integer, IntColumn] =
    new SimpleFilterValueConverter((Short.short2int _).andThen(FilterValue.int).apply)

  implicit val byteFilterValueConverter: FilterValueConverter[Byte, Integer, IntColumn] =
    new SimpleFilterValueConverter((Byte.byte2int _).andThen(FilterValue.int).apply)

  implicit val charFilterValueConverter: FilterValueConverter[Char, Integer, IntColumn] =
    new SimpleFilterValueConverter((Char.char2int _).andThen(FilterValue.int).apply)

  implicit val longFilterValueConverter: FilterValueConverter[Long, java.lang.Long, LongColumn] =
    new SimpleFilterValueConverter(FilterValue.long)

  implicit val floatFilterValueConverter: FilterValueConverter[Float, java.lang.Float, FloatColumn] =
    new SimpleFilterValueConverter(FilterValue.float)

  implicit val doubleFilterValueConverter: FilterValueConverter[Double, java.lang.Double, DoubleColumn] =
    new SimpleFilterValueConverter(FilterValue.double)

  implicit val booleanFilterValueConverter: FilterValueConverter[Boolean, java.lang.Boolean, BooleanColumn] =
    new SimpleFilterValueConverter(FilterValue.boolean)

  implicit val sqlDateFilterValueConverter: FilterValueConverter[java.sql.Date, Integer, IntColumn] =
    new IntFilterValueConverter(ValueCodec.sqlDateCodec)

  implicit val localDateFilterValueConverter: FilterValueConverter[java.time.LocalDate, Integer, IntColumn] =
    new IntFilterValueConverter(ValueCodec.localDateCodec)

  implicit val decimalFilterValueConverter: FilterValueConverter[BigDecimal, Binary, BinaryColumn] =
    new BinaryFilterValueConverter(ValueCodec.decimalCodec)

}

private object FilterValue {

  type FilterValueFactory[V <: Comparable[V], C <: Column[V]] = ValueCodecConfiguration => FilterValue[V, C]

  implicit def convert[In, V <: Comparable[V], C <: Column[V]](in: In)
                                                              (implicit filterValueConverter: FilterValueConverter[In, V, C]): FilterValueFactory[V, C] =
    filterValueConverter.convert(in)

  def binary(binary: Binary): FilterValue[Binary, BinaryColumn] = new FilterValueImpl(binary, FilterApi.binaryColumn)

  def int(int: Int): FilterValue[Integer, IntColumn] = new FilterValueImpl(int, FilterApi.intColumn)

  def long(long: Long): FilterValue[java.lang.Long, LongColumn] = new FilterValueImpl(long, FilterApi.longColumn)

  def float(float: Float): FilterValue[java.lang.Float, FloatColumn] = new FilterValueImpl(float, FilterApi.floatColumn)

  def double(double: Double): FilterValue[java.lang.Double, DoubleColumn] = new FilterValueImpl(double, FilterApi.doubleColumn)

  def boolean(bool: Boolean): FilterValue[java.lang.Boolean, BooleanColumn] = new FilterValueImpl(bool, FilterApi.booleanColumn)

}

private trait FilterValue[V <: Comparable[V], C <: Column[V]] {

  def value: V

  def columnFactory: String => C

}

private class FilterValueImpl[V <: Comparable[V], C <: Column[V]](override val value: V,
                                                                  override val columnFactory: String => C
                                                                 ) extends FilterValue[V, C]

private trait FilterValueSetConverter[In, V <: Comparable[V], C <: Column[V]] {

  def convert(in: Iterable[In]): FilterValueSetFactory[V, C]
}

private class SimpleFilterValueSetConverter[In, V <: Comparable[V], C <: Column[V]](f: Iterable[In] => FilterValueSet[V, C]
                                                                                   ) extends FilterValueSetConverter[In, V, C] {
  override def convert(in: Iterable[In]): FilterValueSetFactory[V, C] = _ => f(in)
}

private class BinaryFilterValueSetConverter[In](codec: ValueCodec[In]) extends FilterValueSetConverter[In, Binary, BinaryColumn] {
  override def convert(in: Iterable[In]): FilterValueSetFactory[Binary, BinaryColumn] =
    conf => FilterValueSet.binary(in.map(codec.encode(_, conf).asInstanceOf[BinaryValue].value))
}

private class IntFilterValueSetConverter[In](codec: ValueCodec[In]) extends FilterValueSetConverter[In, Integer, IntColumn] {
  override def convert(in: Iterable[In]): FilterValueSetFactory[Integer, IntColumn] =
    conf => FilterValueSet.int(in.map(codec.encode(_, conf).asInstanceOf[PrimitiveValue[Int]].value))
}

private object FilterValueSetConverter {
  implicit val stringFilterValueSetConverter: FilterValueSetConverter[String, Binary, BinaryColumn] =
    new BinaryFilterValueSetConverter(ValueCodec.stringCodec)

  implicit val intFilterValueSetConverter: FilterValueSetConverter[Int, Integer, IntColumn] =
    new SimpleFilterValueSetConverter(FilterValueSet.int)

  implicit val shortFilterValueSetConverter: FilterValueSetConverter[Short, Integer, IntColumn] =
    new SimpleFilterValueSetConverter(shorts => FilterValueSet.int(shorts.map(_.toInt)))

  implicit val byteFilterValueSetConverter: FilterValueSetConverter[Byte, Integer, IntColumn] =
    new SimpleFilterValueSetConverter(bytes => FilterValueSet.int(bytes.map(_.toInt)))

  implicit val charFilterValueSetConverter: FilterValueSetConverter[Char, Integer, IntColumn] =
    new SimpleFilterValueSetConverter(chars => FilterValueSet.int(chars.map(_.toInt)))

  implicit val longFilterValueSetConverter: FilterValueSetConverter[Long, java.lang.Long, LongColumn] =
    new SimpleFilterValueSetConverter(FilterValueSet.long)

  implicit val floatFilterValueSetConverter: FilterValueSetConverter[Float, java.lang.Float, FloatColumn] =
    new SimpleFilterValueSetConverter(FilterValueSet.float)

  implicit val doubleFilterValueSetConverter: FilterValueSetConverter[Double, java.lang.Double, DoubleColumn] =
    new SimpleFilterValueSetConverter(FilterValueSet.double)

  implicit val sqlDateFilterValueSetConverter: FilterValueSetConverter[java.sql.Date, Integer, IntColumn] =
    new IntFilterValueSetConverter(ValueCodec.sqlDateCodec)

  implicit val localDateFilterValueSetConverter: FilterValueSetConverter[java.time.LocalDate, Integer, IntColumn] =
    new IntFilterValueSetConverter(ValueCodec.localDateCodec)

  implicit val decimalFilterValueSetConverter: FilterValueSetConverter[BigDecimal, Binary, BinaryColumn] =
    new BinaryFilterValueSetConverter(ValueCodec.decimalCodec)

}

private object FilterValueSet {
  type FilterValueSetFactory[V <: Comparable[V], C <: Column[V]] = ValueCodecConfiguration => FilterValueSet[V, C]

  implicit def convert[In, V <: Comparable[V], C <: Column[V]](in: Iterable[In])
                                                              (implicit filterValueSetConverter: FilterValueSetConverter[In, V, C]): FilterValueSetFactory[V, C] =
    filterValueSetConverter.convert(in)

  def binary(binaries: Iterable[Binary]): FilterValueSet[Binary, BinaryColumn] =
    new FilterValueSetImpl(binaries.to[immutable.Set], FilterApi.binaryColumn)

  def int(ints: Iterable[Int]): FilterValueSet[Integer, IntColumn] =
    new FilterValueSetImpl(ints.map(new Integer(_)).to[immutable.Set], FilterApi.intColumn)

  def long(longs: Iterable[Long]): FilterValueSet[java.lang.Long, LongColumn] =
    new FilterValueSetImpl(longs.map(new java.lang.Long(_)).to[immutable.Set], FilterApi.longColumn)

  def float(floats: Iterable[Float]): FilterValueSet[java.lang.Float, FloatColumn] =
    new FilterValueSetImpl(floats.map(new java.lang.Float(_)).to[immutable.Set], FilterApi.floatColumn)

  def double(doubles: Iterable[Double]): FilterValueSet[java.lang.Double, DoubleColumn] =
    new FilterValueSetImpl(doubles.map(new java.lang.Double(_)).to[immutable.Set], FilterApi.doubleColumn)
}

private trait FilterValueSet[V <: Comparable[V], C <: Column[V]] {

  def value: Set[V]

  def columnFactory: String => C
}

private class FilterValueSetImpl[V <: Comparable[V], C <: Column[V]](override val value: Set[V],
                                                                     override val columnFactory: String => C
                                                                    ) extends FilterValueSet[V, C]
