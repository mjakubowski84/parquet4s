package com.github.mjakubowski84.parquet4s

import java.nio.{ByteBuffer, ByteOrder}
import java.sql.{Date, Timestamp}
import java.util.TimeZone

import scala.language.higherKinds


trait ValueCodec[T] {

  def decode(value: Value): T

  def encode(data: T): Value
}


trait PrimitiveValueCodecs {

  implicit val stringCodec: ValueCodec[String] = new ValueCodec[String] {
    override def decode(value: Value): String =
      value match {
        case StringValue(str) => str
      }
    override def encode(data: String): Value = StringValue(data)
  }

  implicit val booleanCodec: ValueCodec[Boolean] = new ValueCodec[Boolean] {
    override def decode(value: Value): Boolean =
      value match {
        case BooleanValue(b) => b
      }
    override def encode(data: Boolean): Value = BooleanValue(data)
  }

  implicit val intCodec: ValueCodec[Int] = new ValueCodec[Int] {
    override def decode(value: Value): Int =
      value match {
        case IntValue(int) => int
        case LongValue(long) => long.toInt
      }
    override def encode(data: Int): Value = IntValue(data)
  }

  implicit val longCodec: ValueCodec[Long] = new ValueCodec[Long] {
    override def decode(value: Value): Long =
      value match {
        case IntValue(int) => int.toLong
        case LongValue(long) => long
      }
    override def encode(data: Long): Value = LongValue(data)
  }

  implicit val doubleCodec: ValueCodec[Double] = new ValueCodec[Double] {
    override def decode(value: Value): Double =
      value match {
        case DoubleValue(double) => double
        case FloatValue(float) => float.toDouble
      }
    override def encode(data: Double): Value = DoubleValue(data)
  }

  implicit val floatCodec: ValueCodec[Float] = new ValueCodec[Float] {
    override def decode(value: Value): Float =
      value match {
        case DoubleValue(double) => double.toFloat
        case FloatValue(float) => float
      }
    override def encode(data: Float): Value = FloatValue(data)
  }
}

trait TimeValueCodecs {

  /**
    * Uses decoding that is implemented in Apache Spark.
    */
  implicit val timestampCodec: ValueCodec[java.sql.Timestamp] = new ValueCodec[java.sql.Timestamp] {

    override def decode(value: Value): java.sql.Timestamp = {
      value match {
//        case t: java.sql.Timestamp => // TODO there are parquet time formats over there to be checked, too
//          t
//        case d: java.sql.Date =>
//          java.sql.Timestamp.from(d.toInstant)
        case BinaryValue(bs: Array[Byte]) =>
          val buf = ByteBuffer.wrap(bs).order(ByteOrder.LITTLE_ENDIAN)
          val timeOfDayNanos = buf.getLong
          val julianDay = buf.getInt
          val rawTime = DateTimeUtils.fromJulianDay(julianDay, timeOfDayNanos)
          new java.sql.Timestamp(DateTimeUtils.toMillis(rawTime))
      }
    }

    override def encode(data: Timestamp): Value = ???
  }

  /**
    * Uses decoding that is implemented in Apache Spark.
    */
  implicit val dateCodec: ValueCodec[java.sql.Date] = new ValueCodec[java.sql.Date] {

    override def decode(value: Value): java.sql.Date = {
      value match {
//        case t: java.sql.Timestamp => // TODO there are parquet time formats over there to be checked, too
//          java.sql.Date.valueOf(t.toLocalDateTime.toLocalDate)
//        case d: java.sql.Date =>
//          d
        case IntValue(daysSinceEpoch) =>
          new java.sql.Date(DateTimeUtils.daysToMillis(daysSinceEpoch, TimeZone.getDefault))
      }
    }

    override def encode(data: Date): Value = ???
  }

}

trait CollectionValueCodecs {

  implicit def traversableCodec[T, Col[_]](implicit
                                           elementCodec: ValueCodec[T],
                                           collectionTransformer: CollectionTransformer[T, Col]
                                         ): ValueCodec[Col[T]] = new ValueCodec[Col[T]] {
    override def decode(value: Value): Col[T] =
      value match {
        case listRecord: ListParquetRecord =>
          collectionTransformer.to(listRecord.elements.map(elementCodec.decode))
      }

    override def encode(data: Col[T]): Value = {
      val listElements = collectionTransformer.from(data).map(elementCodec.encode)
      ListParquetRecord(listElements:_*)
    }
  }

  implicit def optionCodec[T](implicit elementCodec: ValueCodec[T]): ValueCodec[Option[T]] = new ValueCodec[Option[T]] {
    override def decode(value: Value): Option[T] =
      value match {
        case primitiveValue: PrimitiveValue[T] =>
          Option(primitiveValue.value)
      }

    override def encode(data: Option[T]): Value = {
      data match {
        case Some(t) => elementCodec.encode(t)
        case None => NullValue // TODO write tests for reading and writing null fields!
      }
    }
  }

  implicit def mapCodec[K, V](implicit kt: ValueCodec[K], vt: ValueCodec[V]): ValueCodec[Map[K, V]] = new ValueCodec[Map[K, V]] {
    override def decode(value: Value): Map[K, V] =
      value match {
        case mapParquetRecord: MapParquetRecord =>
          mapParquetRecord.getMap.map { case (mapKey, mapValue) =>
            kt.decode(mapKey) -> vt.decode(mapValue)
          }
      }

    override def encode(data: Map[K, V]): Value = ???
  }

}

trait AllValueCodecs
  extends PrimitiveValueCodecs
    with TimeValueCodecs
    with CollectionValueCodecs
