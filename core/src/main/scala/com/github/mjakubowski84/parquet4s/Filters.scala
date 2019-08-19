package com.github.mjakubowski84.parquet4s

import java.util.TimeZone

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

object FilterValueConverter {

  implicit val stringFilterValueConverter: FilterValueConverter[String, Binary, BinaryColumn] = new FilterValueConverter[String, Binary, BinaryColumn] {
    override def convert(in: String): FilterValueFactory[Binary, BinaryColumn] =
      conf => FilterValue.binary(ValueCodec.stringCodec.encode(in, conf).asInstanceOf[PrimitiveValue[Binary]].value)
  }

  implicit val intFilterValueConverter: FilterValueConverter[Int, Integer, IntColumn] = new FilterValueConverter[Int, Integer, IntColumn] {
    override def convert(in: Int): FilterValueFactory[Integer, IntColumn] =
      _ => FilterValue.int(in)
  }

  implicit val shortFilterValueConverter: FilterValueConverter[Short, Integer, IntColumn] = new FilterValueConverter[Short, Integer, IntColumn] {
    override def convert(in: Short): FilterValueFactory[Integer, IntColumn] =
      _ => FilterValue.int(in.toInt)
  }

  implicit val byteFilterValueConverter: FilterValueConverter[Byte, Integer, IntColumn] = new FilterValueConverter[Byte, Integer, IntColumn] {
    override def convert(in: Byte): FilterValueFactory[Integer, IntColumn] =
      _ => FilterValue.int(in.toInt)
  }

  implicit val charFilterValueConverter: FilterValueConverter[Char, Integer, IntColumn] = new FilterValueConverter[Char, Integer, IntColumn] {
    override def convert(in: Char): FilterValueFactory[Integer, IntColumn] =
      _ => FilterValue.int(in.toInt)
  }

  implicit val longFilterValueConverter: FilterValueConverter[Long, java.lang.Long, LongColumn] = new FilterValueConverter[Long, java.lang.Long, LongColumn] {
    override def convert(in: Long): FilterValueFactory[java.lang.Long, LongColumn] =
      _ => FilterValue.long(in)
  }

  implicit val floatFilterValueConverter: FilterValueConverter[Float, java.lang.Float, FloatColumn] = new FilterValueConverter[Float, java.lang.Float, FloatColumn] {
    override def convert(in: Float): FilterValueFactory[java.lang.Float, FloatColumn] =
      _ => FilterValue.float(in)
  }

  implicit val doubleFilterValueConverter: FilterValueConverter[Double, java.lang.Double, DoubleColumn] = new FilterValueConverter[Double, java.lang.Double, DoubleColumn] {
    override def convert(in: Double): FilterValueFactory[java.lang.Double, DoubleColumn] =
      _ => FilterValue.double(in)
  }

  implicit val booleanFilterValueConverter: FilterValueConverter[Boolean, java.lang.Boolean, BooleanColumn] = new FilterValueConverter[Boolean, java.lang.Boolean, BooleanColumn] {
    override def convert(in: Boolean): FilterValueFactory[java.lang.Boolean, BooleanColumn] =
      _ => FilterValue.boolean(in)
  }
}

object FilterValue {

  type FilterValueFactory[V <: Comparable[V], C <: Column[V]] = ValueCodecConfiguration => FilterValue[V, C]

  implicit def convert[In, V <: Comparable[V], C <: Column[V]](in: In)
                                                              (implicit filterValueConverter: FilterValueConverter[In, V, C]): FilterValueFactory[V, C] =
    filterValueConverter.convert(in)

  def binary(binary: Binary): FilterValue[Binary, BinaryColumn] = new FilterValue[Binary, BinaryColumn] {
    override val value: Binary = binary
    override val columnFactory: String => BinaryColumn = FilterApi.binaryColumn
  }

  def int(int: Integer): FilterValue[Integer, IntColumn] = new FilterValue[Integer, IntColumn] {
    override val value: Integer = int
    override val columnFactory: String => IntColumn = FilterApi.intColumn
  }

  def long(long: Long): FilterValue[java.lang.Long, LongColumn] = new FilterValue[java.lang.Long, LongColumn] {
    override val value: java.lang.Long = long
    override val columnFactory: String => LongColumn = FilterApi.longColumn
  }

  def float(float: Float): FilterValue[java.lang.Float, FloatColumn] = new FilterValue[java.lang.Float, FloatColumn] {
    override val value: java.lang.Float = float
    override val columnFactory: String => FloatColumn = FilterApi.floatColumn
  }

  def double(double: Double): FilterValue[java.lang.Double, DoubleColumn] = new FilterValue[java.lang.Double, DoubleColumn] {
    override val value: java.lang.Double = double
    override val columnFactory: String => DoubleColumn = FilterApi.doubleColumn
  }

  def boolean(bool: Boolean): FilterValue[java.lang.Boolean, BooleanColumn] = new FilterValue[java.lang.Boolean, BooleanColumn] {
    override val value: java.lang.Boolean = bool
    override val columnFactory: String => BooleanColumn = FilterApi.booleanColumn
  }

}

trait FilterValue[V <: Comparable[V], C <: Column[V]] {

  def value: V

  def columnFactory: String => C

}

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