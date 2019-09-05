package com.github.mjakubowski84.parquet4s

import com.github.mjakubowski84.parquet4s.FilterValue.FilterValueFactory
import org.apache.parquet.filter2.predicate.Operators._
import org.apache.parquet.filter2.predicate.{FilterApi, FilterPredicate}
import org.apache.parquet.io.api.Binary

import scala.language.{higherKinds, implicitConversions}


trait Filter {

  def toPredicate(valueCodecConfiguration: ValueCodecConfiguration): FilterPredicate

  def &&(other: Filter): Filter = Filter.andFilter(this, other)

  def |(other: Filter): Filter = Filter.orFilter(this, other)

  def unary_! : Filter = Filter.notFilter(this)

}

object Filter {

  def eqFilter[V <: Comparable[V], C <: Column[V] with SupportsEqNotEq](columnPath: String, expressionFactory: FilterValueFactory[V, C]): Filter = new Filter {
    override def toPredicate(valueCodecConfiguration: ValueCodecConfiguration): FilterPredicate = {
      val expression = expressionFactory(valueCodecConfiguration)
      FilterApi.eq(expression.columnFactory(columnPath), expression.value)
    }
  }

  def neqFilter[V <: Comparable[V], C <: Column[V] with SupportsEqNotEq](columnPath: String, expressionFactory: FilterValueFactory[V, C]): Filter = new Filter {
    override def toPredicate(valueCodecConfiguration: ValueCodecConfiguration): FilterPredicate = {
      val expression = expressionFactory(valueCodecConfiguration)
      FilterApi.notEq(expression.columnFactory(columnPath), expression.value)
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

  def gtFilter[V <: Comparable[V], C <: Column[V] with SupportsLtGt](columnPath: String, expressionFactory: FilterValueFactory[V, C]): Filter = new Filter {
    override def toPredicate(valueCodecConfiguration: ValueCodecConfiguration): FilterPredicate = {
      val expression = expressionFactory(valueCodecConfiguration)
      FilterApi.gt(expression.columnFactory(columnPath), expression.value)
    }
  }

  def gtEqFilter[V <: Comparable[V], C <: Column[V] with SupportsLtGt](columnPath: String, expressionFactory: FilterValueFactory[V, C]): Filter = new Filter {
    override def toPredicate(valueCodecConfiguration: ValueCodecConfiguration): FilterPredicate = {
      val expression = expressionFactory(valueCodecConfiguration)
      FilterApi.gtEq(expression.columnFactory(columnPath), expression.value)
    }
  }

  def ltFilter[V <: Comparable[V], C <: Column[V] with SupportsLtGt](columnPath: String, expressionFactory: FilterValueFactory[V, C]): Filter = new Filter {
    override def toPredicate(valueCodecConfiguration: ValueCodecConfiguration): FilterPredicate = {
      val expression = expressionFactory(valueCodecConfiguration)
      FilterApi.lt(expression.columnFactory(columnPath), expression.value)
    }
  }

  def ltEqFilter[V <: Comparable[V], C <: Column[V] with SupportsLtGt](columnPath: String, expressionFactory: FilterValueFactory[V, C]): Filter = new Filter {
    override def toPredicate(valueCodecConfiguration: ValueCodecConfiguration): FilterPredicate = {
      val expression = expressionFactory(valueCodecConfiguration)
      FilterApi.ltEq(expression.columnFactory(columnPath), expression.value)
    }
  }

}

trait FilterValueConverter[In, V <: Comparable[V], C <: Column[V]] {

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

object FilterValueConverter {

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

object FilterValue {

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

trait FilterValue[V <: Comparable[V], C <: Column[V]] {

  def value: V

  def columnFactory: String => C

}

private class FilterValueImpl[V <: Comparable[V], C <: Column[V]](override val value: V,
                                                                  override val columnFactory: String => C
                                                                 ) extends FilterValue[V, C]

case class Col(columnPath: String) {

  def ===[V <: Comparable[V], C <: Column[V] with SupportsEqNotEq](expressionFactory: FilterValueFactory[V, C]): Filter =
    Filter.eqFilter(columnPath, expressionFactory)

  def !==[V <: Comparable[V], C <: Column[V] with SupportsEqNotEq](expressionFactory: FilterValueFactory[V, C]): Filter =
    Filter.neqFilter(columnPath, expressionFactory)

  def >[V <: Comparable[V], C <: Column[V] with SupportsLtGt](expressionFactory: FilterValueFactory[V, C]): Filter =
    Filter.gtFilter(columnPath, expressionFactory)

  def >=[V <: Comparable[V], C <: Column[V] with SupportsLtGt](expressionFactory: FilterValueFactory[V, C]): Filter =
    Filter.gtEqFilter(columnPath, expressionFactory)

  def <[V <: Comparable[V], C <: Column[V] with SupportsLtGt](expressionFactory: FilterValueFactory[V, C]): Filter =
    Filter.ltFilter(columnPath, expressionFactory)

  def <=[V <: Comparable[V], C <: Column[V] with SupportsLtGt](expressionFactory: FilterValueFactory[V, C]): Filter =
    Filter.ltEqFilter(columnPath, expressionFactory)

}
