package com.github.mjakubowski84.parquet4s

import java.nio.{ByteBuffer, ByteOrder}
import java.sql.{Date, Timestamp}
import java.time._
import java.util.TimeZone

import scala.language.higherKinds


object ValueCodec extends AllValueCodecs

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

object TimeValueCodecs {
  val JulianDayOfEpoch = 2440588
  val MicrosPerMilli = 1000l
  val NanosPerMicro = 1000l
  val NanosPerMilli: Long = NanosPerMicro * MicrosPerMilli
  val NanosPerDay = 86400000000000l
}

trait TimeValueCodecs {

  /**
    * Uses decoding that is implemented in Apache Spark.
    */
  implicit val timestampCodec: ValueCodec[java.sql.Timestamp] = new ValueCodec[java.sql.Timestamp] {

    // TODO there are parquet time formats over there to be checked, too

    import TimeValueCodecs._

    private val timeZone = TimeZone.getDefault // TODO should be configurable

    override def decode(value: Value): java.sql.Timestamp =
      value match {
        case BinaryValue(bs: Array[Byte]) =>
          val buf = ByteBuffer.wrap(bs).order(ByteOrder.LITTLE_ENDIAN)
          val fixedTimeInNanos = buf.getLong
          val julianDay = buf.getInt

          val date = LocalDate.ofEpochDay(julianDay - JulianDayOfEpoch)

          val fixedTimeInMillis = Math.floorDiv(fixedTimeInNanos, NanosPerMilli)
          val nanosLeft = Math.floorMod(fixedTimeInNanos, NanosPerMilli)
          val timeInMillis = fixedTimeInMillis + timeZone.getRawOffset
          val timeInNanos = (timeInMillis * NanosPerMilli) + nanosLeft

          if (timeInNanos >= NanosPerDay) { // fixes issue with Spark
            val time = LocalTime.ofNanoOfDay(timeInNanos - NanosPerDay)
            Timestamp.valueOf(LocalDateTime.of(date.plusDays(1), time))
          } else {
            val time = LocalTime.ofNanoOfDay(timeInNanos)
            Timestamp.valueOf(LocalDateTime.of(date, time))
          }
      }

    override def encode(data: Timestamp): Value = BinaryValue {
      val dateTime = data.toLocalDateTime
      val date = dateTime.toLocalDate
      val time = dateTime.toLocalTime

      val julianDay = JulianDayOfEpoch + date.toEpochDay.toInt

      val timeInNanos = time.toNanoOfDay
      val timeInMillis = Math.floorDiv(timeInNanos, NanosPerMilli)
      val nanosLeft = Math.floorMod(timeInNanos, NanosPerMilli)
      val fixedTimeInMillis = timeInMillis - timeZone.getRawOffset
      val fixedTimeInNanos = fixedTimeInMillis * NanosPerMilli + nanosLeft

      val buf = ByteBuffer.allocate(12).order(ByteOrder.LITTLE_ENDIAN)
      buf.putLong(fixedTimeInNanos)
      buf.putInt(julianDay)
      buf.array()
    }
  }

  /**
    * Uses decoding that is implemented in Apache Spark.
    */
  implicit val dateCodec: ValueCodec[java.sql.Date] = new ValueCodec[java.sql.Date] {

    // TODO there are parquet time formats over there to be checked, too

    override def decode(value: Value): java.sql.Date =
      value match {
        case IntValue(epochDay) =>
          Date.valueOf(LocalDate.ofEpochDay(epochDay))
      }

    override def encode(data: Date): Value = IntValue(
      data.toLocalDate.toEpochDay.toInt
    )
  }

}

trait ComplexValueCodecs {

  implicit def collectionCodec[T, Col[_]](implicit
                                          elementCodec: ValueCodec[T],
                                          collectionTransformer: CollectionTransformer[T, Col]
                                         ): ValueCodec[Col[T]] = new ValueCodec[Col[T]] {
    override def decode(value: Value): Col[T] =
      value match {
        case listRecord: ListParquetRecord =>
          collectionTransformer.to(listRecord.elements.map(elementCodec.decode))
      }

    override def encode(data: Col[T]): Value =
      collectionTransformer.from(data).map(elementCodec.encode).foldLeft(ListParquetRecord.empty) {
        case (record, element) =>
          record.add(element)
      }
  }

  implicit def optionCodec[T](implicit elementCodec: ValueCodec[T]): ValueCodec[Option[T]] = new ValueCodec[Option[T]] {
    override def decode(value: Value): Option[T] =
      value match {
        case NullValue => None
        case _ => Option(elementCodec.decode(value))
      }

    override def encode(data: Option[T]): Value =
      data match {
        case None => NullValue // TODO write tests for reading and writing null fields!
        case Some(t) => elementCodec.encode(t)
      }
  }

  implicit def mapCodec[K, V](implicit
                              kCodec: ValueCodec[K],
                              vCodec: ValueCodec[V]
                             ): ValueCodec[Map[K, V]] = new ValueCodec[Map[K, V]] {
    override def decode(value: Value): Map[K, V] =
      value match {
        case mapParquetRecord: MapParquetRecord =>
          mapParquetRecord.getMap.map { case (mapKey, mapValue) =>
            kCodec.decode(mapKey) -> vCodec.decode(mapValue)
          }
      }

    override def encode(data: Map[K, V]): Value =
      data.foldLeft(MapParquetRecord.empty) { case (record, (key, value)) =>
        record.add(kCodec.encode(key), vCodec.encode(value))
      }
  }

  implicit def productCodec[T](implicit
                               encoder: ParquetRecordEncoder[T],
                               decoder: ParquetRecordDecoder[T]
                              ): ValueCodec[T] = new ValueCodec[T] {

    override def decode(value: Value): T = decoder.decode(value)
    override def encode(data: T): Value = encoder.encode(data)
  }

}

trait AllValueCodecs
  extends PrimitiveValueCodecs
    with TimeValueCodecs
    with ComplexValueCodecs
