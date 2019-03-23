package com.github.mjakubowski84.parquet4s

import java.nio.{ByteBuffer, ByteOrder}
import java.time._
import java.util.TimeZone

import scala.language.higherKinds

/**
  * Contains implicit instances of all [[ValueCodec]]
  */
object ValueCodec extends AllValueCodecs

/**
  * Type class that allows to decode data from Parquet [[Value]] or encode it as [[Value]]
  * @tparam T data type to decode to or encode from
  */
trait ValueCodec[T] {

  /**
    * @param value source Parquet [[Value]]
    * @param configuration [ValueCodecConfiguration] used by some codecs
    * @return data decoded from [[Value]]
    */
  def decode(value: Value, configuration: ValueCodecConfiguration): T

  /**
    * @param data source data
    * @param configuration [ValueCodecConfiguration] used by some codecs
    * @return encoded Parquet [[Value]]
    */
  def encode(data: T, configuration: ValueCodecConfiguration): Value

}


object ValueCodecConfiguration {
  val default: ValueCodecConfiguration = ValueCodecConfiguration(TimeZone.getDefault)
}

/**
  * Configuration necessary for some of codecs
  * @param timeZone used when encoding and decoding time-based values
  */
case class ValueCodecConfiguration(timeZone: TimeZone)

/**
  * Codec for non-null type of [[Value]]
  * @tparam T data type to decode to or encode from
  */
trait RequiredValueCodec[T] extends ValueCodec[T] {

  final override def decode(value: Value, configuration: ValueCodecConfiguration): T =
    value match {
      case NullValue =>
        throw new IllegalArgumentException("NullValue cannot be decoded to required type")
      case other =>
        decodeNonNull(other, configuration)
    }

  final override def encode(data: T, configuration: ValueCodecConfiguration): Value =
    Option(data) match {
      case None =>
        throw new IllegalArgumentException("Cannot encode null instance of required type")
      case Some(other) =>
        encodeNonNull(other, configuration)
    }

  protected def decodeNonNull(value: Value, configuration: ValueCodecConfiguration): T

  protected def encodeNonNull(data: T, configuration: ValueCodecConfiguration): Value

}

/**
  * Codec for [[Value]] that can be null.
  * @tparam T data type to decode to or encode from
  */
trait OptionalValueCodec[T] extends ValueCodec[T] {

  final override def decode(value: Value, configuration: ValueCodecConfiguration): T =
    value match {
      case NullValue => null.asInstanceOf[T]
      case other => decodeNonNull(other, configuration)
    }

  final override def encode(data: T, configuration: ValueCodecConfiguration): Value =
    Option(data).fold[Value](NullValue)(nonNullData => encodeNonNull(nonNullData, configuration))

  protected def decodeNonNull(value: Value, configuration: ValueCodecConfiguration): T

  protected def encodeNonNull(data: T, configuration: ValueCodecConfiguration): Value

}


trait PrimitiveValueCodecs {

  implicit val stringCodec: ValueCodec[String] = new OptionalValueCodec[String] {
    override def decodeNonNull(value: Value, configuration: ValueCodecConfiguration): String =
      value match {
        case StringValue(str) => str
      }
    override def encodeNonNull(data: String, configuration: ValueCodecConfiguration): Value = StringValue(data)
  }

  implicit val booleanCodec: ValueCodec[Boolean] = new RequiredValueCodec[Boolean] {
    override def decodeNonNull(value: Value, configuration: ValueCodecConfiguration): Boolean =
      value match {
        case BooleanValue(b) => b
      }
    override def encodeNonNull(data: Boolean, configuration: ValueCodecConfiguration): Value = BooleanValue(data)
  }

  implicit val intCodec: ValueCodec[Int] = new RequiredValueCodec[Int] {
    override def decodeNonNull(value: Value, configuration: ValueCodecConfiguration): Int =
      value match {
        case IntValue(int) => int
        case LongValue(long) => long.toInt
      }
    override def encodeNonNull(data: Int, configuration: ValueCodecConfiguration): Value = IntValue(data)
  }

  implicit val longCodec: ValueCodec[Long] = new RequiredValueCodec[Long] {
    override def decodeNonNull(value: Value, configuration: ValueCodecConfiguration): Long =
      value match {
        case IntValue(int) => int.toLong
        case LongValue(long) => long
      }
    override def encodeNonNull(data: Long, configuration: ValueCodecConfiguration): Value = LongValue(data)
  }

  implicit val doubleCodec: ValueCodec[Double] = new RequiredValueCodec[Double] {
    override def decodeNonNull(value: Value, configuration: ValueCodecConfiguration): Double =
      value match {
        case DoubleValue(double) => double
        case FloatValue(float) => float.toDouble
      }
    override def encodeNonNull(data: Double, configuration: ValueCodecConfiguration): Value = DoubleValue(data)
  }

  implicit val floatCodec: ValueCodec[Float] = new RequiredValueCodec[Float] {
    override def decodeNonNull(value: Value, configuration: ValueCodecConfiguration): Float =
      value match {
        case DoubleValue(double) => double.toFloat
        case FloatValue(float) => float
      }
    override def encodeNonNull(data: Float, configuration: ValueCodecConfiguration): Value = FloatValue(data)
  }

  implicit val shortCodec: ValueCodec[Short] = new RequiredValueCodec[Short] {
    override def decodeNonNull(value: Value, configuration: ValueCodecConfiguration): Short =
      value match {
        case ShortValue(short) => short
        case IntValue(int) => int.toShort
      }
    override def encodeNonNull(data: Short, configuration: ValueCodecConfiguration): Value = ShortValue(data)
  }

  implicit val byteCodec: ValueCodec[Byte] = new RequiredValueCodec[Byte] {
    override def decodeNonNull(value: Value, configuration: ValueCodecConfiguration): Byte =
      value match {
        case ByteValue(byte) => byte
        case IntValue(int) => int.toByte
      }
    override def encodeNonNull(data: Byte, configuration: ValueCodecConfiguration): Value = ByteValue(data)
  }

}

object TimeValueCodecs {
  val JulianDayOfEpoch = 2440588
  val MicrosPerMilli = 1000l
  val NanosPerMicro = 1000l
  val NanosPerMilli: Long = NanosPerMicro * MicrosPerMilli
  val NanosPerDay = 86400000000000l

  // TODO there are parquet time formats over there to be checked, too

  private def decodeLocalDateTime(value: Value, configuration: ValueCodecConfiguration): LocalDateTime =
    value match {
      case BinaryValue(bs: Array[Byte]) =>
        val buf = ByteBuffer.wrap(bs).order(ByteOrder.LITTLE_ENDIAN)
        val fixedTimeInNanos = buf.getLong
        val julianDay = buf.getInt

        val date = LocalDate.ofEpochDay(julianDay - JulianDayOfEpoch)

        val fixedTimeInMillis = Math.floorDiv(fixedTimeInNanos, NanosPerMilli)
        val nanosLeft = Math.floorMod(fixedTimeInNanos, NanosPerMilli)
        val timeInMillis = fixedTimeInMillis + configuration.timeZone.getRawOffset
        val timeInNanos = (timeInMillis * NanosPerMilli) + nanosLeft

        if (timeInNanos >= NanosPerDay) {
          /*
           * original value was encoded with time zone WEST to one that we read it with
           * and we experience a day flip due to difference in time zone offset
           */
          val time = LocalTime.ofNanoOfDay(timeInNanos - NanosPerDay)
          LocalDateTime.of(date.plusDays(1), time)
        } else if (timeInNanos < 0) {
          /*
           * original value was encoded with time zone EAST to one that we read it with
           * and we experience a day flip due to difference in time zone offset
           */
          val time = LocalTime.ofNanoOfDay(timeInNanos + NanosPerDay)
          LocalDateTime.of(date.minusDays(1), time)
        } else {
          val time = LocalTime.ofNanoOfDay(timeInNanos)
          LocalDateTime.of(date, time)
        }
    }

  private def encodeLocalDateTime(data:LocalDateTime, configuration: ValueCodecConfiguration): Value = BinaryValue {
    val date = data.toLocalDate
    val time = data.toLocalTime

    val julianDay = JulianDayOfEpoch + date.toEpochDay.toInt

    val timeInNanos = time.toNanoOfDay
    val timeInMillis = Math.floorDiv(timeInNanos, NanosPerMilli)
    val nanosLeft = Math.floorMod(timeInNanos, NanosPerMilli)
    val fixedTimeInMillis = timeInMillis - configuration.timeZone.getRawOffset
    val fixedTimeInNanos = fixedTimeInMillis * NanosPerMilli + nanosLeft

    val buf = ByteBuffer.allocate(12).order(ByteOrder.LITTLE_ENDIAN)
    buf.putLong(fixedTimeInNanos)
    buf.putInt(julianDay)
    buf.array()
  }

  /**
    * Uses decoding that is implemented in Apache Spark.
    */
  private def decodeLocalDate(value: Value): LocalDate =
    value match {
      case IntValue(epochDay) => LocalDate.ofEpochDay(epochDay)
    }

  /**
    * Uses decoding that is implemented in Apache Spark.
    */
  private def encodeLocalDate(data:LocalDate): Value = IntValue(data.toEpochDay.toInt)

}

trait TimeValueCodecs {

  implicit val localDateTimeCodec: ValueCodec[LocalDateTime] =
    new OptionalValueCodec[LocalDateTime] {

    override def decodeNonNull(value: Value, configuration: ValueCodecConfiguration): LocalDateTime =
      TimeValueCodecs.decodeLocalDateTime(value, configuration)

    override def encodeNonNull(data: LocalDateTime, configuration: ValueCodecConfiguration): Value =
      TimeValueCodecs.encodeLocalDateTime(data, configuration)
  }

  implicit val sqlTimestampCodec: ValueCodec[java.sql.Timestamp] = new OptionalValueCodec[java.sql.Timestamp] {

    override def decodeNonNull(value: Value, configuration: ValueCodecConfiguration): java.sql.Timestamp = {
      val dateTime = TimeValueCodecs.decodeLocalDateTime(value, configuration)
      java.sql.Timestamp.valueOf(dateTime)
    }

    override def encodeNonNull(data: java.sql.Timestamp, configuration: ValueCodecConfiguration): Value =
      TimeValueCodecs.encodeLocalDateTime(data.toLocalDateTime, configuration)
  }


  implicit val localDateCodec: ValueCodec[LocalDate] = new OptionalValueCodec[LocalDate] {

    override def decodeNonNull(value: Value, configuration: ValueCodecConfiguration): LocalDate =
      TimeValueCodecs.decodeLocalDate(value)

    override def encodeNonNull(data: LocalDate, configuration: ValueCodecConfiguration): Value =
      TimeValueCodecs.encodeLocalDate(data)
  }

  implicit val sqlDateCodec: ValueCodec[java.sql.Date] = new OptionalValueCodec[java.sql.Date] {

    override def decodeNonNull(value: Value, configuration: ValueCodecConfiguration): java.sql.Date = {
      val date = TimeValueCodecs.decodeLocalDate(value)
      java.sql.Date.valueOf(date)
    }

    override def encodeNonNull(data: java.sql.Date, configuration: ValueCodecConfiguration): Value =
      TimeValueCodecs.encodeLocalDate(data.toLocalDate)
  }

}

trait ComplexValueCodecs {

  implicit def collectionCodec[T, Col[_]](implicit
                                          elementCodec: ValueCodec[T],
                                          collectionTransformer: CollectionTransformer[T, Col]
                                         ): ValueCodec[Col[T]] = new OptionalValueCodec[Col[T]] {
    override def decodeNonNull(value: Value, configuration: ValueCodecConfiguration): Col[T] =
      value match {
        case listRecord: ListParquetRecord =>
          collectionTransformer.to(listRecord.elements.map((elementCodec.decode _).curried(_)(configuration)))
      }

    override def encodeNonNull(data: Col[T], configuration: ValueCodecConfiguration): Value =
      collectionTransformer
        .from(data)
        .map((elementCodec.encode _).curried(_)(configuration))
        .foldLeft(ListParquetRecord.empty) {
          case (record, element) =>
            record.add(element)
        }
  }

  implicit def optionCodec[T](implicit elementCodec: ValueCodec[T]): ValueCodec[Option[T]] = new ValueCodec[Option[T]] {
    override def decode(value: Value, configuration: ValueCodecConfiguration): Option[T] =
      value match {
        case NullValue => None
        case _ => Option(elementCodec.decode(value, configuration))
      }

    override def encode(data: Option[T], configuration: ValueCodecConfiguration): Value =
      data match {
        case None => NullValue
        case Some(t) => elementCodec.encode(t, configuration)
      }
  }

  implicit def mapCodec[K, V](implicit
                              kCodec: ValueCodec[K],
                              vCodec: ValueCodec[V]
                             ): ValueCodec[Map[K, V]] = new OptionalValueCodec[Map[K, V]] {
    override def decodeNonNull(value: Value, configuration: ValueCodecConfiguration): Map[K, V] =
      value match {
        case mapParquetRecord: MapParquetRecord =>
          mapParquetRecord.getMap.map { case (mapKey, mapValue) =>
            require(mapKey != NullValue, "Map cannot have null keys")
            kCodec.decode(mapKey, configuration) -> vCodec.decode(mapValue, configuration)
          }
      }

    override def encodeNonNull(data: Map[K, V], configuration: ValueCodecConfiguration): Value =
      data.foldLeft(MapParquetRecord.empty) { case (record, (key, value)) =>
        require(key != null, "Map cannot have null keys")
        record.add(kCodec.encode(key, configuration), vCodec.encode(value, configuration))
      }
  }

  implicit def productCodec[T](implicit
                               encoder: ParquetRecordEncoder[T],
                               decoder: ParquetRecordDecoder[T]
                              ): ValueCodec[T] = new OptionalValueCodec[T] {

    override def decodeNonNull(value: Value, configuration: ValueCodecConfiguration): T =
      value match {
        case record: RowParquetRecord =>
          decoder.decode(record, configuration)
      }

    override def encodeNonNull(data: T, configuration: ValueCodecConfiguration): Value =
      encoder.encode(data, configuration)
  }

}

trait AllValueCodecs
  extends PrimitiveValueCodecs
    with TimeValueCodecs
    with ComplexValueCodecs
