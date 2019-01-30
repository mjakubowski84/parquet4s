package com.github.mjakubowski84.parquet4s

import java.nio.charset.StandardCharsets

import org.apache.parquet.io.api.{Binary, RecordConsumer}
import org.apache.parquet.schema.Type

trait Value {

  def write(schema: Type, recordConsumer: RecordConsumer): Unit

}

trait PrimitiveValue[T] extends Value {

  def value: T

}

case class StringValue(value: String) extends PrimitiveValue[String] {

  def this(binary: Binary) = this(binary.toStringUsingUTF8)

  override def write(schema: Type, recordConsumer: RecordConsumer): Unit =
    recordConsumer.addBinary(Binary.fromReusedByteArray(value.getBytes(StandardCharsets.UTF_8)))

}

case class LongValue(value: Long) extends PrimitiveValue[Long] {
  override def write(schema: Type, recordConsumer: RecordConsumer): Unit = recordConsumer.addLong(value)
}

case class IntValue(value: Int) extends PrimitiveValue[Int] {
  override def write(schema: Type, recordConsumer: RecordConsumer): Unit = recordConsumer.addInteger(value)
}

case class FloatValue(value: Float) extends PrimitiveValue[Float] {
  override def write(schema: Type, recordConsumer: RecordConsumer): Unit = recordConsumer.addFloat(value)
}

case class DoubleValue(value: Double) extends PrimitiveValue[Double] {
  override def write(schema: Type, recordConsumer: RecordConsumer): Unit = recordConsumer.addDouble(value)
}

case class BinaryValue(value: Array[Byte]) extends PrimitiveValue[Array[Byte]] {

  def this(binary: Binary) = this(binary.getBytes)

  override def write(schema: Type, recordConsumer: RecordConsumer): Unit = recordConsumer.addBinary(Binary.fromReusedByteArray(value))

  override def equals(obj: Any): Boolean =
    obj match {
      case other @ BinaryValue(otherValue) =>
        (other canEqual this) && value.sameElements(otherValue)
      case _ => false
    }
}

case class BooleanValue(value: Boolean) extends PrimitiveValue[Boolean] {
  override def write(schema: Type, recordConsumer: RecordConsumer): Unit = recordConsumer.addBoolean(value)
}

case object NullValue extends Value {
  override def write(schema: Type, recordConsumer: RecordConsumer): Unit =
    throw new UnsupportedOperationException("Null values cannot be written.")
}
