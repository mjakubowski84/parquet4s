package com.github.mjakubowski84.parquet4s

import java.nio.{ByteBuffer, ByteOrder}
import java.time._
import java.util.TimeZone

import scala.language.higherKinds


object ValueCodec extends AllValueCodecs

trait ValueCodec[T] {

  def decode(value: Value): T

  def encode(data: T): Value

}

trait RequiredValueCodec[T] extends ValueCodec[T] {

  final override def decode(value: Value): T =
    value match {
      case NullValue =>
        throw new IllegalArgumentException("NullValue cannot be decoded to required type")
      case other =>
        decodeNonNull(other)
    }

  final override def encode(data: T): Value =
    Option(data) match {
      case None =>
        throw new IllegalArgumentException("Cannot encode null instance of required type")
      case Some(other) =>
        encodeNonNull(other)
    }

  protected def decodeNonNull(value: Value): T

  protected def encodeNonNull(data: T): Value

}

trait OptionalValueCodec[T] extends ValueCodec[T] {

  final override def decode(value: Value): T =
    value match {
      case NullValue => null.asInstanceOf[T]
      case other => decodeNonNull(other)
    }

  final override def encode(data: T): Value =
    Option(data).fold[Value](NullValue)(encodeNonNull)

  protected def decodeNonNull(value: Value): T

  protected def encodeNonNull(data: T): Value

}


trait PrimitiveValueCodecs {

  implicit val stringCodec: ValueCodec[String] = new OptionalValueCodec[String] {
    override def decodeNonNull(value: Value): String =
      value match {
        case StringValue(str) => str
      }
    override def encodeNonNull(data: String): Value = StringValue(data)
  }

  implicit val booleanCodec: ValueCodec[Boolean] = new RequiredValueCodec[Boolean] {
    override def decodeNonNull(value: Value): Boolean =
      value match {
        case BooleanValue(b) => b
      }
    override def encodeNonNull(data: Boolean): Value = BooleanValue(data)
  }

  implicit val intCodec: ValueCodec[Int] = new RequiredValueCodec[Int] {
    override def decodeNonNull(value: Value): Int =
      value match {
        case IntValue(int) => int
        case LongValue(long) => long.toInt
      }
    override def encodeNonNull(data: Int): Value = IntValue(data)
  }

  implicit val longCodec: ValueCodec[Long] = new RequiredValueCodec[Long] {
    override def decodeNonNull(value: Value): Long =
      value match {
        case IntValue(int) => int.toLong
        case LongValue(long) => long
      }
    override def encodeNonNull(data: Long): Value = LongValue(data)
  }

  implicit val doubleCodec: ValueCodec[Double] = new RequiredValueCodec[Double] {
    override def decodeNonNull(value: Value): Double =
      value match {
        case DoubleValue(double) => double
        case FloatValue(float) => float.toDouble
      }
    override def encodeNonNull(data: Double): Value = DoubleValue(data)
  }

  implicit val floatCodec: ValueCodec[Float] = new RequiredValueCodec[Float] {
    override def decodeNonNull(value: Value): Float =
      value match {
        case DoubleValue(double) => double.toFloat
        case FloatValue(float) => float
      }
    override def encodeNonNull(data: Float): Value = FloatValue(data)
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
  implicit val timestampCodec: ValueCodec[java.sql.Timestamp] = new OptionalValueCodec[java.sql.Timestamp] {

    // TODO there are parquet time formats over there to be checked, too

    import TimeValueCodecs._

    private val timeZone = TimeZone.getDefault // TODO should be configurable

    override def decodeNonNull(value: Value): java.sql.Timestamp =
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

          if (timeInNanos >= NanosPerDay) { // fixes issue with Spark when in number of nanos >= 1 day
            val time = LocalTime.ofNanoOfDay(timeInNanos - NanosPerDay)
            java.sql.Timestamp.valueOf(LocalDateTime.of(date.plusDays(1), time))
          } else {
            val time = LocalTime.ofNanoOfDay(timeInNanos)
            java.sql.Timestamp.valueOf(LocalDateTime.of(date, time))
          }
      }

    override def encodeNonNull(data: java.sql.Timestamp): Value = BinaryValue {
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
  implicit val dateCodec: ValueCodec[java.sql.Date] = new OptionalValueCodec[java.sql.Date] {

    // TODO there are parquet time formats over there to be checked, too

    override def decodeNonNull(value: Value): java.sql.Date =
      value match {
        case IntValue(epochDay) =>
          java.sql.Date.valueOf(LocalDate.ofEpochDay(epochDay))
      }

    override def encodeNonNull(data: java.sql.Date): Value = IntValue(
      data.toLocalDate.toEpochDay.toInt
    )
  }

}

trait ComplexValueCodecs {

  implicit def collectionCodec[T, Col[_]](implicit
                                          elementCodec: ValueCodec[T],
                                          collectionTransformer: CollectionTransformer[T, Col]
                                         ): ValueCodec[Col[T]] = new OptionalValueCodec[Col[T]] {
    override def decodeNonNull(value: Value): Col[T] =
      value match {
        case listRecord: ListParquetRecord =>
          collectionTransformer.to(listRecord.elements.map(elementCodec.decode))
      }

    override def encodeNonNull(data: Col[T]): Value =
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
                             ): ValueCodec[Map[K, V]] = new OptionalValueCodec[Map[K, V]] {
    override def decodeNonNull(value: Value): Map[K, V] =
      value match {
        case mapParquetRecord: MapParquetRecord =>
          mapParquetRecord.getMap.map { case (mapKey, mapValue) =>
            require(mapKey != NullValue, "Map cannot have null keys")
            kCodec.decode(mapKey) -> vCodec.decode(mapValue)
          }
      }

    override def encodeNonNull(data: Map[K, V]): Value =
      data.foldLeft(MapParquetRecord.empty) { case (record, (key, value)) =>
        require(key != null, "Map cannot have null keys")
        record.add(kCodec.encode(key), vCodec.encode(value))
      }
  }

  implicit def productCodec[T](implicit
                               encoder: ParquetRecordEncoder[T],
                               decoder: ParquetRecordDecoder[T]
                              ): ValueCodec[T] = new OptionalValueCodec[T] {

    override def decodeNonNull(value: Value): T = decoder.decode(value)
    override def encodeNonNull(data: T): Value = encoder.encode(data)
  }

}

trait AllValueCodecs
  extends PrimitiveValueCodecs
    with TimeValueCodecs
    with ComplexValueCodecs
