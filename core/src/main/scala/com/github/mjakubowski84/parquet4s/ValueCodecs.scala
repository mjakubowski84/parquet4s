package com.github.mjakubowski84.parquet4s

import java.nio.{ByteBuffer, ByteOrder}
import java.sql.{Date, Timestamp}
import java.time.*
import scala.collection.compat.*
import scala.language.higherKinds
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
        case DoubleValue(double) => double
        case FloatValue(float)   => float.toDouble
      }
  }

  implicit val floatDecoder: RequiredValueDecoder[Float] = new RequiredValueDecoder[Float] {
    def decodeNonNull(value: Value, configuration: ValueCodecConfiguration): Float =
      value match {
        case DoubleValue(double) => double.toFloat
        case FloatValue(float)   => float
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

  implicit val decimalDecoder: OptionalValueDecoder[BigDecimal] = new OptionalValueDecoder[BigDecimal] {
    def decodeNonNull(value: Value, configuration: ValueCodecConfiguration): BigDecimal =
      value match {
        case IntValue(int)       => BigDecimal(int)
        case LongValue(long)     => BigDecimal.decimal(long)
        case DoubleValue(double) => BigDecimal.decimal(double)
        case FloatValue(float)   => BigDecimal.decimal(float)
        case BinaryValue(binary) => Decimals.decimalFromBinary(binary)
      }
  }
}
trait PrimitiveValueEncoders {

  implicit val stringEncoder: OptionalValueEncoder[String] = new OptionalValueEncoder[String] {
    def encodeNonNull(data: String, configuration: ValueCodecConfiguration): Value = BinaryValue(data)
  }

  implicit val charEncoder: RequiredValueEncoder[Char] = new RequiredValueEncoder[Char] {
    def encodeNonNull(data: Char, configuration: ValueCodecConfiguration): Value = IntValue(data)
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
    def encodeNonNull(data: Short, configuration: ValueCodecConfiguration): Value = IntValue(data)
  }

  implicit val byteEncoder: RequiredValueEncoder[Byte] = new RequiredValueEncoder[Byte] {
    def encodeNonNull(data: Byte, configuration: ValueCodecConfiguration): Value = IntValue(data)
  }

  implicit val decimalEncoder: OptionalValueEncoder[BigDecimal] = new OptionalValueEncoder[BigDecimal] {
    def encodeNonNull(data: BigDecimal, configuration: ValueCodecConfiguration): Value =
      BinaryValue(Decimals.binaryFromDecimal(data))
  }
}

private[parquet4s] object TimeValueCodecs {
  val JulianDayOfEpoch    = 2440588
  val MicrosPerMilli      = 1000L
  val NanosPerMicro       = 1000L
  val NanosPerMilli: Long = NanosPerMicro * MicrosPerMilli
  val NanosPerDay         = 86400000000000L

  // TODO there are other parquet time formats over there to be checked, too

  def decodeLocalDateTime(value: Value, configuration: ValueCodecConfiguration): LocalDateTime =
    value match {
      case BinaryValue(binary) =>
        val buf              = ByteBuffer.wrap(binary.getBytes).order(ByteOrder.LITTLE_ENDIAN)
        val fixedTimeInNanos = buf.getLong
        val julianDay        = buf.getInt

        val date = LocalDate.ofEpochDay(julianDay - JulianDayOfEpoch)

        val fixedTimeInMillis = Math.floorDiv(fixedTimeInNanos, NanosPerMilli)
        val nanosLeft         = Math.floorMod(fixedTimeInNanos, NanosPerMilli)
        val timeInMillis      = fixedTimeInMillis + configuration.timeZone.getRawOffset
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
    }

  def encodeLocalDateTime(data: LocalDateTime, configuration: ValueCodecConfiguration): Value = BinaryValue {
    val date = data.toLocalDate
    val time = data.toLocalTime

    val julianDay = JulianDayOfEpoch + date.toEpochDay.toInt

    val timeInNanos       = time.toNanoOfDay
    val timeInMillis      = Math.floorDiv(timeInNanos, NanosPerMilli)
    val nanosLeft         = Math.floorMod(timeInNanos, NanosPerMilli)
    val fixedTimeInMillis = timeInMillis - configuration.timeZone.getRawOffset
    val fixedTimeInNanos  = fixedTimeInMillis * NanosPerMilli + nanosLeft

    val buf = ByteBuffer.allocate(12).order(ByteOrder.LITTLE_ENDIAN)
    buf.putLong(fixedTimeInNanos)
    buf.putInt(julianDay)
    buf.array()
  }

  def decodeLocalDate(value: Value): LocalDate =
    value match {
      case IntValue(epochDay) => LocalDate.ofEpochDay(epochDay)
    }

  def encodeLocalDate(data: LocalDate): Value = IntValue(data.toEpochDay.toInt)

}

trait TimeValueDecoders {

  implicit val localDateTimeDecoder: OptionalValueDecoder[LocalDateTime] = new OptionalValueDecoder[LocalDateTime] {
    def decodeNonNull(value: Value, configuration: ValueCodecConfiguration): LocalDateTime =
      TimeValueCodecs.decodeLocalDateTime(value, configuration)
  }

  implicit val sqlTimestampDecoder: OptionalValueDecoder[java.sql.Timestamp] = new OptionalValueDecoder[Timestamp] {
    def decodeNonNull(value: Value, configuration: ValueCodecConfiguration): Timestamp =
      java.sql.Timestamp.valueOf(TimeValueCodecs.decodeLocalDateTime(value, configuration))
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
      TimeValueCodecs.encodeLocalDateTime(data, configuration)
  }

  implicit val sqlTimestampEncoder: OptionalValueEncoder[java.sql.Timestamp] = new OptionalValueEncoder[Timestamp] {
    def encodeNonNull(data: Timestamp, configuration: ValueCodecConfiguration): Value =
      TimeValueCodecs.encodeLocalDateTime(data.toLocalDateTime, configuration)
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
      evidence: Col[T] <:< Iterable[T],
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
      evidence: Col[T] =:= Array[T],
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
