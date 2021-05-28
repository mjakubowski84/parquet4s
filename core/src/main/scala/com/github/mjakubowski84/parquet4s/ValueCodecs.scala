package com.github.mjakubowski84.parquet4s

import java.nio.{ByteBuffer, ByteOrder}
import java.time._

import scala.collection.compat._
import scala.language.higherKinds
import scala.reflect.ClassTag


trait PrimitiveValueDecoders {

  implicit val stringDecoder: OptionalValueDecoder[String] = (value, _) =>
    value match {
      case BinaryValue(binary) => binary.toStringUsingUTF8
    }

  implicit val charDecoder: RequiredValueDecoder[Char] = (value, _) =>
    value match {
      case IntValue(int) => int.toChar
    }

  implicit val booleanDecoder: RequiredValueDecoder[Boolean] = (value, _) =>
    value match {
      case BooleanValue(b) => b
    }

  implicit val intDecoder: RequiredValueDecoder[Int] = (value, _) =>
    value match {
      case IntValue(int) => int
      case LongValue(long) => long.toInt
    }

  implicit val longDecoder: RequiredValueDecoder[Long] = (value, _) =>
    value match {
      case IntValue(int) => int.toLong
      case LongValue(long) => long
    }

  implicit val doubleDecoder: RequiredValueDecoder[Double] = (value, _) =>
    value match {
      case DoubleValue(double) => double
      case FloatValue(float) => float.toDouble
    }

  implicit val floatDecoder: RequiredValueDecoder[Float] = (value, _) =>
    value match {
      case DoubleValue(double) => double.toFloat
      case FloatValue(float) => float
    }

  implicit val shortDecoder: RequiredValueDecoder[Short] = (value, _) =>
    value match {
      case IntValue(int) => int.toShort
    }

  implicit val byteDecoder: RequiredValueDecoder[Byte] = (value, _) =>
    value match {
      case IntValue(int) => int.toByte
    }

  implicit val decimalDecoder: OptionalValueDecoder[BigDecimal] = (value, _) =>
    value match {
      case IntValue(int) => BigDecimal(int)
      case LongValue(long) => BigDecimal.decimal(long)
      case DoubleValue(double) => BigDecimal.decimal(double)
      case FloatValue(float) => BigDecimal.decimal(float)
      case BinaryValue(binary) => Decimals.decimalFromBinary(binary)
    }
}
trait PrimitiveValueEncoders {

  implicit val stringEncoder: OptionalValueEncoder[String] = (data, _) => BinaryValue(data)

  implicit val charEncoder: RequiredValueEncoder[Char] = (data, _) => IntValue(data)

  implicit val booleanEncoder: RequiredValueEncoder[Boolean] = (data, _)=> BooleanValue(data)

  implicit val intEncoder: RequiredValueEncoder[Int] = (data, _) => IntValue(data)

  implicit val longEncoder: RequiredValueEncoder[Long] = (data, _) => LongValue(data)

  implicit val doubleEncoder: RequiredValueEncoder[Double] = (data, _) => DoubleValue(data)

  implicit val floatEncoder: RequiredValueEncoder[Float] = (data, _) => FloatValue(data)

  implicit val shortEncoder: RequiredValueEncoder[Short] = (data, _) => IntValue(data)

  implicit val byteEncoder: RequiredValueEncoder[Byte] = (data, _) => IntValue(data)

  implicit val decimalEncoder: OptionalValueEncoder[BigDecimal] = (data, _) => BinaryValue(Decimals.binaryFromDecimal(data))
}

private[parquet4s] object TimeValueCodecs {
  val JulianDayOfEpoch = 2440588
  val MicrosPerMilli = 1000L
  val NanosPerMicro = 1000L
  val NanosPerMilli: Long = NanosPerMicro * MicrosPerMilli
  val NanosPerDay = 86400000000000L

  // TODO there are other parquet time formats over there to be checked, too

  def decodeLocalDateTime(value: Value, configuration: ValueCodecConfiguration): LocalDateTime =
    value match {
      case BinaryValue(binary) =>
        val buf = ByteBuffer.wrap(binary.getBytes).order(ByteOrder.LITTLE_ENDIAN)
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

  def encodeLocalDateTime(data: LocalDateTime, configuration: ValueCodecConfiguration): Value = BinaryValue {
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

  def decodeLocalDate(value: Value): LocalDate =
    value match {
      case IntValue(epochDay) => LocalDate.ofEpochDay(epochDay)
    }

  def encodeLocalDate(data:LocalDate): Value = IntValue(data.toEpochDay.toInt)

}

trait TimeValueDecoders {

  implicit val localDateTimeDecoder: OptionalValueDecoder[LocalDateTime] =
    (value, configuration) => TimeValueCodecs.decodeLocalDateTime(value, configuration)

  implicit val sqlTimestampDecoder: OptionalValueDecoder[java.sql.Timestamp] =
    (value, configuration) => java.sql.Timestamp.valueOf(TimeValueCodecs.decodeLocalDateTime(value, configuration))

  implicit val localDateDecoder: OptionalValueDecoder[LocalDate] =
    (value, _) => TimeValueCodecs.decodeLocalDate(value)

  implicit val sqlDateDecoder: OptionalValueDecoder[java.sql.Date] =
    (value, _) => java.sql.Date.valueOf(TimeValueCodecs.decodeLocalDate(value))

}

trait TimeValueEncoders {

  implicit val localDateTimeEncoder: OptionalValueEncoder[LocalDateTime] =
    (data, configuration) => TimeValueCodecs.encodeLocalDateTime(data, configuration)

  implicit val sqlTimestampEncoder: OptionalValueEncoder[java.sql.Timestamp] =
    (data, configuration) => TimeValueCodecs.encodeLocalDateTime(data.toLocalDateTime, configuration)

  implicit val localDateEncoder: OptionalValueEncoder[LocalDate] =
    (data, _) => TimeValueCodecs.encodeLocalDate(data)

  implicit val sqlDateEncoder: OptionalValueEncoder[java.sql.Date] =
    (data, _) => TimeValueCodecs.encodeLocalDate(data.toLocalDate)

}

trait ComplexValueDecoders {

  implicit def collectionDecoder[T, Col[_]](implicit
                                            evidence: Col[T] <:< Iterable[T],
                                            elementDecoder: ValueDecoder[T],
                                            factory: Factory[T, Col[T]]
                                           ): OptionalValueDecoder[Col[T]] =
    (value, configuration) =>
      value match {
        case listRecord: ListParquetRecord =>
          listRecord.map((elementDecoder.decode _).curried(_)(configuration)).to(factory)
      }

  implicit def arrayDecoder[T, Col[_]](implicit
                                       evidence: Col[T] =:= Array[T],
                                       classTag: ClassTag[T],
                                       factory: Factory[T, Col[T]],
                                       elementDecoder: ValueDecoder[T]
                                      ): OptionalValueDecoder[Col[T]] =
    (value, configuration) =>
      value match {
        case listRecord: ListParquetRecord =>
          listRecord.map((elementDecoder.decode _).curried(_)(configuration)).to(factory)
        case binaryValue: BinaryValue if classTag.runtimeClass == classOf[Byte] =>
          binaryValue.value.getBytes.asInstanceOf[Col[T]]
      }

  implicit def optionDecoder[T](implicit elementDecoder: ValueDecoder[T]): ValueDecoder[Option[T]] =
    (value, configuration) =>
      value match {
        case NullValue => None
        case _ => Option(elementDecoder.decode(value, configuration))
      }

  implicit def mapDecoder[K, V](implicit
                                kDecoder: ValueDecoder[K],
                                vDecoder: ValueDecoder[V]
                               ): OptionalValueDecoder[Map[K, V]] =
    (value, configuration) =>
      value match {
        case mapParquetRecord: MapParquetRecord =>
          mapParquetRecord.map { case (mapKey, mapValue) =>
            require(mapKey != NullValue, "Map cannot have null keys")
            kDecoder.decode(mapKey, configuration) -> vDecoder.decode(mapValue, configuration)
          }
      }

  implicit def productDecoder[T](implicit decoder: ParquetRecordDecoder[T]): OptionalValueDecoder[T] =
    (value, configuration) =>
      value match {
        case record: RowParquetRecord => decoder.decode(record, configuration)
      }

}

trait ComplexValueEncoders {

  implicit def collectionEncoder[T, Col[_]](implicit
                                            evidence: Col[T] <:< Iterable[T],
                                            elementEncoder: ValueEncoder[T]
                                           ): OptionalValueEncoder[Col[T]] =
    (data, configuration) =>
      evidence(data)
        .foldLeft(ListParquetRecord.Empty) {
          case (record, element) => record.appended(element, configuration)
        }

  implicit def arrayEncoder[T, Col[_]](implicit
                                       evidence: Col[T] =:= Array[T],
                                       classTag: ClassTag[T],
                                       elementEncoder: ValueEncoder[T],
                                      ): OptionalValueEncoder[Col[T]] =
    (data, configuration) =>
      if (classTag.runtimeClass == classOf[Byte])
        BinaryValue(data.asInstanceOf[Array[Byte]])
      else
        evidence(data)
          .foldLeft(ListParquetRecord.Empty) {
            case (record, element) => record.appended(element, configuration)
          }

  implicit def optionEncoder[T](implicit elementEncoder: ValueEncoder[T]): ValueEncoder[Option[T]] =
    (data, configuration) =>
      data match {
        case None => NullValue
        case Some(t) => elementEncoder.encode(t, configuration)
      }

  implicit def mapEncoder[K, V](implicit
                                kEncoder: ValueEncoder[K],
                                vEncoder: ValueEncoder[V]
                               ): OptionalValueEncoder[Map[K, V]] =
    (data, configuration) =>
      data.foldLeft(MapParquetRecord.Empty) { case (record, (key, value)) =>
        require(key != null, "Map cannot have null keys")
        record.updated(key, value, configuration)
      }

  implicit def productEncoder[T](implicit encoder: ParquetRecordEncoder[T]): OptionalValueEncoder[T] =
    (data, configuration) => encoder.encode(data, configuration)

}
