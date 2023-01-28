package com.github.mjakubowski84.parquet4s

import com.github.mjakubowski84.parquet4s.TimeValueCodecs.*
import org.apache.parquet.filter2.predicate.Operators.LongColumn
import org.apache.parquet.schema.LogicalTypeAnnotation.TimestampLogicalTypeAnnotation
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT64

import java.sql.Timestamp
import java.time.{LocalDateTime, ZonedDateTime}

object TimestampFormat {

  sealed trait Format

  case object Int64Millis extends Format
  case object Int64Micros extends Format
  case object Int64Nanos extends Format
  case object Int96 extends Format

  object Implicits {

    abstract protected class base(
        logicalTypeAnnotation: TimestampLogicalTypeAnnotation,
        timestampFormat: TimestampFormat.Format
    ) {
      implicit val sqlTimestampSchemaDef: TypedSchemaDef[Timestamp] =
        SchemaDef
          .primitive(INT64, required = false, logicalTypeAnnotation = Option(logicalTypeAnnotation))
          .typed[java.sql.Timestamp]

      implicit val localDateTimeSchemaDef: TypedSchemaDef[LocalDateTime] =
        SchemaDef
          .primitive(INT64, required = false, logicalTypeAnnotation = Option(logicalTypeAnnotation))
          .typed[LocalDateTime]

      implicit def sqlTimestampEncoder: OptionalValueEncoder[Timestamp]

      implicit def localDateTimeEncoder: OptionalValueEncoder[LocalDateTime]

      implicit val sqlTimestampFilterCodec: FilterCodec[Timestamp, java.lang.Long, LongColumn] =
        FilterCodec[Timestamp, java.lang.Long, LongColumn](
          encode = sqlTimestampEncoder.encode(_, _).asInstanceOf[PrimitiveValue[Long]].value,
          decode = (v, vcc) => ValueDecoder.sqlTimestampDecoder.decode(DateTimeValue(v, timestampFormat), vcc)
        )

      implicit val localDateTimeFilterCodec: FilterCodec[LocalDateTime, java.lang.Long, LongColumn] =
        FilterCodec[LocalDateTime, java.lang.Long, LongColumn](
          encode = localDateTimeEncoder.encode(_, _).asInstanceOf[PrimitiveValue[Long]].value,
          decode = (v, vcc) => ValueDecoder.localDateTimeDecoder.decode(DateTimeValue(v, timestampFormat), vcc)
        )

    }

    object Millis extends base(LogicalTypes.TimestampMillisType, Int64Millis) {

      implicit override val sqlTimestampEncoder: OptionalValueEncoder[Timestamp] = new OptionalValueEncoder[Timestamp] {
        def encodeNonNull(data: Timestamp, configuration: ValueCodecConfiguration): Value =
          DateTimeValue(data.getTime, Int64Millis)
      }

      implicit override val localDateTimeEncoder: OptionalValueEncoder[LocalDateTime] =
        new OptionalValueEncoder[LocalDateTime] {
          def encodeNonNull(data: LocalDateTime, configuration: ValueCodecConfiguration): Value = {
            val zoned = ZonedDateTime.of(data, configuration.timeZone.toZoneId)
            DateTimeValue(zoned.toInstant.toEpochMilli, Int64Millis)
          }
        }

    }

    object Micros extends base(LogicalTypes.TimestampMicrosType, Int64Micros) {

      implicit override val sqlTimestampEncoder: OptionalValueEncoder[Timestamp] = new OptionalValueEncoder[Timestamp] {
        def encodeNonNull(data: Timestamp, configuration: ValueCodecConfiguration): Value = {
          val instant = data.toInstant
          val micros  = (instant.getEpochSecond * MicrosPerSecond) + (instant.getNano / NanosPerMicro)
          DateTimeValue(micros, Int64Micros)
        }
      }

      implicit override val localDateTimeEncoder: OptionalValueEncoder[LocalDateTime] =
        new OptionalValueEncoder[LocalDateTime] {
          def encodeNonNull(data: LocalDateTime, configuration: ValueCodecConfiguration): Value = {
            val zoned  = ZonedDateTime.of(data, configuration.timeZone.toZoneId)
            val micros = (zoned.toEpochSecond * MicrosPerSecond) + (zoned.getNano / NanosPerMicro)
            DateTimeValue(micros, Int64Micros)
          }
        }

    }

    object Nanos extends base(LogicalTypes.TimestampNanosType, Int64Nanos) {

      implicit override val sqlTimestampEncoder: OptionalValueEncoder[Timestamp] = new OptionalValueEncoder[Timestamp] {
        def encodeNonNull(data: Timestamp, configuration: ValueCodecConfiguration): Value = {
          val instant = data.toInstant
          val nanos   = (instant.getEpochSecond * MicrosPerSecond * NanosPerMicro) + instant.getNano
          DateTimeValue(nanos, Int64Micros)
        }
      }

      implicit override val localDateTimeEncoder: OptionalValueEncoder[LocalDateTime] =
        new OptionalValueEncoder[LocalDateTime] {
          def encodeNonNull(data: LocalDateTime, configuration: ValueCodecConfiguration): Value = {
            val zoned = ZonedDateTime.of(data, configuration.timeZone.toZoneId)
            val nanos = (zoned.toEpochSecond * MicrosPerSecond * NanosPerMicro) + zoned.getNano
            DateTimeValue(nanos, Int64Micros)
          }
        }
    }
  }

}
