package com.github.mjakubowski84.parquet4s

import org.apache.parquet.io.api.{Binary, RecordConsumer}
import org.apache.parquet.schema.Type

/**
  * Basic structure element which Parquet data is built from. Represents any data element that can be read from or
  * can be written to Parquet files.
  */
trait Value extends Any {

  /**
    * Writes the value content to Parquet
    * @param schema schema of that value
    * @param recordConsumer has to be used to write the data to the file
    */
  def write(schema: Type, recordConsumer: RecordConsumer): Unit

}

/**
  * Primitive value like integer or long.
  * @tparam T type of the value
  */
trait PrimitiveValue[T] extends Any with Value {

  /**
    * Content of the value
    */
  def value: T

}

case class LongValue(value: Long) extends AnyVal with PrimitiveValue[Long] {
  override def write(schema: Type, recordConsumer: RecordConsumer): Unit = recordConsumer.addLong(value)
}

case class IntValue(value: Int) extends AnyVal with PrimitiveValue[Int] {
  override def write(schema: Type, recordConsumer: RecordConsumer): Unit = recordConsumer.addInteger(value)
}

case class FloatValue(value: Float) extends AnyVal with PrimitiveValue[Float] {
  override def write(schema: Type, recordConsumer: RecordConsumer): Unit = recordConsumer.addFloat(value)
}

case class DoubleValue(value: Double) extends AnyVal with PrimitiveValue[Double] {
  override def write(schema: Type, recordConsumer: RecordConsumer): Unit = recordConsumer.addDouble(value)
}

object BinaryValue {
  def apply(bytes: Array[Byte]): BinaryValue = BinaryValue(Binary.fromReusedByteArray(bytes))
}

case class BinaryValue(value: Binary) extends PrimitiveValue[Binary] {

  override def write(schema: Type, recordConsumer: RecordConsumer): Unit = recordConsumer.addBinary(value)

}

case class BooleanValue(value: Boolean) extends AnyVal with PrimitiveValue[Boolean] {
  override def write(schema: Type, recordConsumer: RecordConsumer): Unit = recordConsumer.addBoolean(value)
}

/**
  * Special instance of [[Value]] that represents lack of the value.
  * [[NullValue]] does not hold any data so it cannot be written.
  */
case object NullValue extends Value {
  override def write(schema: Type, recordConsumer: RecordConsumer): Unit =
    throw new UnsupportedOperationException("Null values cannot be written.")
}
