package com.github.mjakubowski84.parquet4s

import com.github.mjakubowski84.parquet4s.TimeValueCodecs.{localDateTimeToTimestamp, timestampToLocalDateTime}

import java.nio.{ByteBuffer, ByteOrder}
import java.sql.{Date, Timestamp}
import java.time.*
import java.util.TimeZone
import scala.annotation.nowarn
import scala.collection.compat.*
import scala.reflect.ClassTag

trait PrimitiveValueDecoders {

  implicit val stringDecoder: OptionalValueDecoder[String] = new OptionalValueDecoder[String] {
    def decodeNonNull(value: Value, configuration: ValueCodecConfiguration): String =
      value match {
        case BinaryValue(binary) => binary.toStringUsingUTF8
      }
  }

  implicit val charDecoder: RequiredValueDecoder[Char] = new RequiredValueDecoder[Char] {
    def decodeNonNull(value: Value, configuration: ValueCodecConfiguration): Char =
      value match {
        case IntValue(int) => int.toChar
      }
  }

  implicit val booleanDecoder: RequiredValueDecoder[Boolean] = new RequiredValueDecoder[Boolean] {
    def decodeNonNull(value: Value, configuration: ValueCodecConfiguration): Boolean =
      value match {
        case BooleanValue(b) => b
      }
  }

  implicit val intDecoder: RequiredValueDecoder[Int] = new RequiredValueDecoder[Int] {
    def decodeNonNull(value: Value, configuration: ValueCodecConfiguration): Int =
      value match {
        case IntValue(int)   => int
        case LongValue(long) => long.toInt
      }
  }

  implicit val longDecoder: RequiredValueDecoder[Long] = new RequiredValueDecoder[Long] {
    def decodeNonNull(value: Value, configuration: ValueCodecConfiguration): Long =
      value match {
        case IntValue(int)   => int.toLong
        case LongValue(long) => long
      }
  }

  implicit val doubleDecoder: RequiredValueDecoder[Double] = new RequiredValueDecoder[Double] {
    def decodeNonNull(value: Value, configuration: ValueCodecConfiguration): Double =
      value match {
        case DoubleValue(double)        => double
        case FloatValue(float)          => float.toDouble
        case decimalValue: DecimalValue => decimalValue.toBigDecimal.toDouble
      }
  }

  implicit val floatDecoder: RequiredValueDecoder[Float] = new RequiredValueDecoder[Float] {
    def decodeNonNull(value: Value, configuration: ValueCodecConfiguration): Float =
      value match {
        case DoubleValue(double)        => double.toFloat
        case FloatValue(float)          => float
        case decimalValue: DecimalValue => decimalValue.toBigDecimal.toFloat
      }
  }

  implicit val shortDecoder: RequiredValueDecoder[Short] = new RequiredValueDecoder[Short] {
    def decodeNonNull(value: Value, configuration: ValueCodecConfiguration): Short =
      value match {
        case IntValue(int) => int.toShort
      }
  }

  implicit val byteDecoder: RequiredValueDecoder[Byte] = new RequiredValueDecoder[Byte] {
    def decodeNonNull(value: Value, configuration: ValueCodecConfiguration): Byte =
      value match {
        case IntValue(int) => int.toByte
      }
  }

  implicit val decimalDecoder: OptionalValueDecoder[BigDecimal] = DecimalFormat.Default.Implicits.decimalDecoder

}
trait PrimitiveValueEncoders {

  implicit val stringEncoder: OptionalValueEncoder[String] = new OptionalValueEncoder[String] {
    def encodeNonNull(data: String, configuration: ValueCodecConfiguration): Value = BinaryValue(data)
  }

  implicit val charEncoder: RequiredValueEncoder[Char] = new RequiredValueEncoder[Char] {
    def encodeNonNull(data: Char, configuration: ValueCodecConfiguration): Value = IntValue(data.toInt)
  }

  implicit val booleanEncoder: RequiredValueEncoder[Boolean] = new RequiredValueEncoder[Boolean] {
    def encodeNonNull(data: Boolean, configuration: ValueCodecConfiguration): Value = BooleanValue(data)
  }

  implicit val intEncoder: RequiredValueEncoder[Int] = new RequiredValueEncoder[Int] {
    def encodeNonNull(data: Int, configuration: ValueCodecConfiguration): Value = IntValue(data)
  }

  implicit val longEncoder: RequiredValueEncoder[Long] = new RequiredValueEncoder[Long] {
    def encodeNonNull(data: Long, configuration: ValueCodecConfiguration): Value = LongValue(data)
  }

  implicit val doubleEncoder: RequiredValueEncoder[Double] = new RequiredValueEncoder[Double] {
    def encodeNonNull(data: Double, configuration: ValueCodecConfiguration): Value = DoubleValue(data)
  }

  implicit val floatEncoder: RequiredValueEncoder[Float] = new RequiredValueEncoder[Float] {
    def encodeNonNull(data: Float, configuration: ValueCodecConfiguration): Value = FloatValue(data)
  }

  implicit val shortEncoder: RequiredValueEncoder[Short] = new RequiredValueEncoder[Short] {
    def encodeNonNull(data: Short, configuration: ValueCodecConfiguration): Value = IntValue(data.toInt)
  }

  implicit val byteEncoder: RequiredValueEncoder[Byte] = new RequiredValueEncoder[Byte] {
    def encodeNonNull(data: Byte, configuration: ValueCodecConfiguration): Value = IntValue(data.toInt)
  }

  implicit val decimalEncoder: OptionalValueEncoder[BigDecimal] = DecimalFormat.Default.Implicits.decimalEncoder
}

private[parquet4s] object TimeValueCodecs {
  val JulianDayOfEpoch      = 2440588
  val MillisPerSecond       = 1000L
  val MicrosPerMilli        = 1000L
  val NanosPerMicro         = 1000L
  val MicrosPerSecond: Long = MicrosPerMilli * MillisPerSecond
  val NanosPerMilli: Long   = NanosPerMicro * MicrosPerMilli
  val NanosPerSecond: Long  = NanosPerMilli * MillisPerSecond
  val NanosPerDay           = 86400000000000L

  def decodeLocalDateTime(value: Value, timeZone: TimeZone): LocalDateTime =
    value match {
      case BinaryValue(binary) =>
        val buf              = ByteBuffer.wrap(binary.getBytes).order(ByteOrder.LITTLE_ENDIAN)
        val fixedTimeInNanos = buf.getLong
        val julianDay        = buf.getInt

        val date = LocalDate.ofEpochDay((julianDay - JulianDayOfEpoch).toLong)

        val fixedTimeInMillis = Math.floorDiv(fixedTimeInNanos, NanosPerMilli)
        val nanosLeft         = Math.floorMod(fixedTimeInNanos, NanosPerMilli)
        val timeInMillis      = fixedTimeInMillis + timeZone.getRawOffset
        val timeInNanos       = (timeInMillis * NanosPerMilli) + nanosLeft

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

      case DateTimeValue(value, TimestampFormat.Int64Millis) =>
        LocalDateTime.ofInstant(Instant.ofEpochMilli(value), timeZone.toZoneId)

      case DateTimeValue(value, TimestampFormat.Int64Micros) =>
        val seconds = value / MicrosPerSecond
        val micros  = value % MicrosPerSecond
        val nanos   = micros * NanosPerMicro
        LocalDateTime.ofInstant(Instant.ofEpochSecond(seconds, nanos), timeZone.toZoneId)

      case DateTimeValue(value, TimestampFormat.Int64Nanos) =>
        val seconds = value / NanosPerSecond
        val nanos   = value % NanosPerSecond
        LocalDateTime.ofInstant(Instant.ofEpochSecond(seconds, nanos), timeZone.toZoneId)
    }

  def decodeInstant(value: Value): Instant =
    value match {
      case BinaryValue(binary) =>
        val buf     = ByteBuffer.wrap(binary.getBytes).order(ByteOrder.LITTLE_ENDIAN)
        val seconds = buf.getLong
        val nanos   = buf.getInt.toLong

        Instant.ofEpochSecond(seconds, nanos)

      case DateTimeValue(value, TimestampFormat.Int64Millis) =>
        Instant.ofEpochMilli(value)

      case DateTimeValue(value, TimestampFormat.Int64Micros) =>
        val seconds = value / MicrosPerSecond
        val micros  = value % MicrosPerSecond
        val nanos   = micros * NanosPerMicro
        Instant.ofEpochSecond(seconds, nanos)

      case DateTimeValue(value, TimestampFormat.Int64Nanos) =>
        val seconds = value / NanosPerSecond
        val nanos   = value % NanosPerSecond
        Instant.ofEpochSecond(seconds, nanos)

    }

  def encodeLocalDateTime(data: LocalDateTime, timeZone: TimeZone): Value = BinaryValue {
    val date = data.toLocalDate
    val time = data.toLocalTime

    val julianDay = JulianDayOfEpoch + date.toEpochDay.toInt

    val timeInNanos       = time.toNanoOfDay
    val timeInMillis      = Math.floorDiv(timeInNanos, NanosPerMilli)
    val nanosLeft         = Math.floorMod(timeInNanos, NanosPerMilli)
    val fixedTimeInMillis = timeInMillis - timeZone.getRawOffset
    val fixedTimeInNanos  = fixedTimeInMillis * NanosPerMilli + nanosLeft

    val buf = ByteBuffer.allocate(12).order(ByteOrder.LITTLE_ENDIAN)
    buf.putLong(fixedTimeInNanos)
    buf.putInt(julianDay)
    buf.array()
  }

  def encodeInstant(instant: Instant): Value = BinaryValue {
    val seconds = instant.getEpochSecond
    val nanos   = instant.getNano

    val buf = ByteBuffer.allocate(12).order(ByteOrder.LITTLE_ENDIAN)
    buf.putLong(seconds)
    buf.putInt(nanos)
    buf.array()
  }

  def decodeLocalDate(value: Value): LocalDate =
    value match {
      case IntValue(epochDay) => LocalDate.ofEpochDay(epochDay.toLong)
    }

  def encodeLocalDate(data: LocalDate): Value = IntValue(data.toEpochDay.toInt)

  def localDateTimeToTimestamp(dateTime: LocalDateTime, timeZone: TimeZone): Timestamp =
    Timestamp.from(ZonedDateTime.of(dateTime, timeZone.toZoneId).toInstant)

  def timestampToLocalDateTime(timestamp: Timestamp, timeZone: TimeZone): LocalDateTime =
    LocalDateTime.ofInstant(timestamp.toInstant, timeZone.toZoneId)
}

trait TimeValueDecoders {

  implicit val localDateTimeDecoder: OptionalValueDecoder[LocalDateTime] = new OptionalValueDecoder[LocalDateTime] {
    def decodeNonNull(value: Value, configuration: ValueCodecConfiguration): LocalDateTime =
      TimeValueCodecs.decodeLocalDateTime(value, configuration.timeZone)
  }

  implicit val instantDecoder: OptionalValueDecoder[Instant] = new OptionalValueDecoder[Instant] {
    def decodeNonNull(value: Value, configuration: ValueCodecConfiguration): Instant =
      TimeValueCodecs.decodeInstant(value)
  }

  implicit val sqlTimestampDecoder: OptionalValueDecoder[java.sql.Timestamp] = new OptionalValueDecoder[Timestamp] {
    def decodeNonNull(value: Value, configuration: ValueCodecConfiguration): Timestamp = {
      val timeZone = configuration.timeZone
      localDateTimeToTimestamp(TimeValueCodecs.decodeLocalDateTime(value, timeZone), timeZone)
    }
  }

  implicit val localDateDecoder: OptionalValueDecoder[LocalDate] = new OptionalValueDecoder[LocalDate] {
    def decodeNonNull(value: Value, configuration: ValueCodecConfiguration): LocalDate =
      TimeValueCodecs.decodeLocalDate(value)
  }

  implicit val sqlDateDecoder: OptionalValueDecoder[java.sql.Date] = new OptionalValueDecoder[Date] {
    def decodeNonNull(value: Value, configuration: ValueCodecConfiguration): Date =
      java.sql.Date.valueOf(TimeValueCodecs.decodeLocalDate(value))
  }

}

trait TimeValueEncoders {

  implicit val localDateTimeEncoder: OptionalValueEncoder[LocalDateTime] = new OptionalValueEncoder[LocalDateTime] {
    def encodeNonNull(data: LocalDateTime, configuration: ValueCodecConfiguration): Value =
      TimeValueCodecs.encodeLocalDateTime(data, configuration.timeZone)
  }

  implicit val instantEncoder: OptionalValueEncoder[Instant] = new OptionalValueEncoder[Instant] {
    def encodeNonNull(data: Instant, configuration: ValueCodecConfiguration): Value =
      TimeValueCodecs.encodeInstant(data)
  }

  implicit val sqlTimestampEncoder: OptionalValueEncoder[java.sql.Timestamp] = new OptionalValueEncoder[Timestamp] {
    def encodeNonNull(data: Timestamp, configuration: ValueCodecConfiguration): Value = {
      val timeZone = configuration.timeZone
      TimeValueCodecs.encodeLocalDateTime(timestampToLocalDateTime(data, timeZone), timeZone)
    }
  }

  implicit val localDateEncoder: OptionalValueEncoder[LocalDate] = new OptionalValueEncoder[LocalDate] {
    def encodeNonNull(data: LocalDate, configuration: ValueCodecConfiguration): Value =
      TimeValueCodecs.encodeLocalDate(data)
  }

  implicit val sqlDateEncoder: OptionalValueEncoder[java.sql.Date] = new OptionalValueEncoder[Date] {
    def encodeNonNull(data: Date, configuration: ValueCodecConfiguration): Value =
      TimeValueCodecs.encodeLocalDate(data.toLocalDate)
  }

}

trait ComplexValueDecoders extends ProductDecoders {

  implicit def collectionDecoder[T, Col[_]](implicit
      @nowarn evidence: Col[T] <:< Iterable[T],
      elementDecoder: ValueDecoder[T],
      factory: Factory[T, Col[T]]
  ): OptionalValueDecoder[Col[T]] =
    new OptionalValueDecoder[Col[T]] {
      def decodeNonNull(value: Value, configuration: ValueCodecConfiguration): Col[T] =
        value match {
          case listRecord: ListParquetRecord =>
            listRecord.map((elementDecoder.decode _).curried(_)(configuration)).to(factory)
        }
    }

  implicit def arrayDecoder[T, Col[_]](implicit
      @nowarn evidence: Col[T] =:= Array[T],
      classTag: ClassTag[T],
      factory: Factory[T, Col[T]],
      elementDecoder: ValueDecoder[T]
  ): OptionalValueDecoder[Col[T]] =
    new OptionalValueDecoder[Col[T]] {
      def decodeNonNull(value: Value, configuration: ValueCodecConfiguration): Col[T] =
        value match {
          case listRecord: ListParquetRecord =>
            listRecord.map((elementDecoder.decode _).curried(_)(configuration)).to(factory)
          case binaryValue: BinaryValue if classTag.runtimeClass == classOf[Byte] =>
            binaryValue.value.getBytes.asInstanceOf[Col[T]]
        }
    }

  implicit def optionDecoder[T](implicit elementDecoder: ValueDecoder[T]): ValueDecoder[Option[T]] =
    new ValueDecoder[Option[T]] {
      def decode(value: Value, configuration: ValueCodecConfiguration): Option[T] =
        value match {
          case NullValue => None
          case _         => Option(elementDecoder.decode(value, configuration))
        }
    }

  implicit def mapDecoder[K, V](implicit
      kDecoder: ValueDecoder[K],
      vDecoder: ValueDecoder[V]
  ): OptionalValueDecoder[Map[K, V]] =
    new OptionalValueDecoder[Map[K, V]] {
      def decodeNonNull(value: Value, configuration: ValueCodecConfiguration): Map[K, V] =
        value match {
          case mapParquetRecord: MapParquetRecord =>
            mapParquetRecord.map { case (mapKey, mapValue) =>
              require(mapKey != NullValue, "Map cannot have null keys")
              kDecoder.decode(mapKey, configuration) -> vDecoder.decode(mapValue, configuration)
            }
        }
    }

}

trait ComplexValueEncoders extends ProductEncoders {

  implicit def collectionEncoder[T, Col[_]](implicit
      evidence: Col[T] <:< Iterable[T],
      elementEncoder: ValueEncoder[T]
  ): OptionalValueEncoder[Col[T]] =
    new OptionalValueEncoder[Col[T]] {
      def encodeNonNull(data: Col[T], configuration: ValueCodecConfiguration): Value =
        evidence(data)
          .foldLeft(ListParquetRecord.Empty) { case (record, element) =>
            record.appended(element, configuration)
          }
    }

  implicit def arrayEncoder[T, Col[_]](implicit
      evidence: Col[T] =:= Array[T],
      classTag: ClassTag[T],
      elementEncoder: ValueEncoder[T]
  ): OptionalValueEncoder[Col[T]] =
    new OptionalValueEncoder[Col[T]] {
      def encodeNonNull(data: Col[T], configuration: ValueCodecConfiguration): Value =
        if (classTag.runtimeClass == classOf[Byte])
          BinaryValue(data.asInstanceOf[Array[Byte]])
        else
          evidence(data)
            .foldLeft(ListParquetRecord.Empty) { case (record, element) =>
              record.appended(element, configuration)
            }
    }

  implicit def optionEncoder[T](implicit elementEncoder: ValueEncoder[T]): ValueEncoder[Option[T]] =
    new ValueEncoder[Option[T]] {
      def encode(data: Option[T], configuration: ValueCodecConfiguration): Value =
        data match {
          case None    => NullValue
          case Some(t) => elementEncoder.encode(t, configuration)
        }
    }

  implicit def mapEncoder[K, V](implicit
      kEncoder: ValueEncoder[K],
      vEncoder: ValueEncoder[V]
  ): OptionalValueEncoder[Map[K, V]] =
    new OptionalValueEncoder[Map[K, V]] {
      def encodeNonNull(data: Map[K, V], configuration: ValueCodecConfiguration): Value =
        data.foldLeft(MapParquetRecord.Empty) { case (record, (key, value)) =>
          require(key != null, "Map cannot have null keys")
          record.updated(key, value, configuration)
        }
    }

}
