package com.github.mjakubowski84.parquet4s

import org.apache.parquet.filter2.predicate.Operators.LongColumn
import org.apache.parquet.schema.LogicalTypeAnnotation.TimestampLogicalTypeAnnotation
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT64

import java.sql.Timestamp
import java.time.{Instant, LocalDateTime, ZonedDateTime}
import java.util.concurrent.TimeUnit

object TimestampFormat {

  sealed trait Format

  case object Int64Millis extends Format
  case object Int64Micros extends Format
  case object Int64Nanos extends Format
  case object Int96 extends Format

  private val NanosPerMicro   = TimeUnit.MICROSECONDS.toNanos(1)
  private val MicrosPerSecond = TimeUnit.SECONDS.toMicros(1)

  object Implicits {

    abstract protected class base(logicalTypeAnnotation: TimestampLogicalTypeAnnotation) {
      implicit val sqlTimestampSchemaDef: TypedSchemaDef[Timestamp] =
        SchemaDef
          .primitive(INT64, required = false, logicalTypeAnnotation = Option(logicalTypeAnnotation))
          .withMetadata(SchemaDef.Meta.Generated)
          .typed[java.sql.Timestamp]

      implicit val localDateTimeSchemaDef: TypedSchemaDef[LocalDateTime] =
        SchemaDef
          .primitive(INT64, required = false, logicalTypeAnnotation = Option(logicalTypeAnnotation))
          .withMetadata(SchemaDef.Meta.Generated)
          .typed[LocalDateTime]

      implicit val instantSchemaDef: TypedSchemaDef[Instant] =
        SchemaDef
          .primitive(INT64, required = false, logicalTypeAnnotation = Option(logicalTypeAnnotation))
          .withMetadata(SchemaDef.Meta.Generated)
          .typed[Instant]

      implicit def sqlTimestampEncoder: OptionalValueEncoder[Timestamp]

      implicit def localDateTimeEncoder: OptionalValueEncoder[LocalDateTime]

      implicit def instantEncoder: OptionalValueEncoder[Instant]

      implicit val sqlTimestampFilterEncoder: FilterEncoder[Timestamp, java.lang.Long, LongColumn] =
        FilterEncoder[Timestamp, java.lang.Long, LongColumn](
          encode = sqlTimestampEncoder.encode(_, _).asInstanceOf[DateTimeValue].value
        )

      implicit val localDateTimeFilterEncoder: FilterEncoder[LocalDateTime, java.lang.Long, LongColumn] =
        FilterEncoder[LocalDateTime, java.lang.Long, LongColumn](
          encode = localDateTimeEncoder.encode(_, _).asInstanceOf[DateTimeValue].value
        )

      implicit val instantFilterEncoder: FilterEncoder[Instant, java.lang.Long, LongColumn] =
        FilterEncoder[Instant, java.lang.Long, LongColumn](
          encode = instantEncoder.encode(_, _).asInstanceOf[DateTimeValue].value
        )
    }

    object Millis extends base(LogicalTypes.TimestampMillisType) {

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

      implicit val instantEncoder: OptionalValueEncoder[Instant] =
        new OptionalValueEncoder[Instant] {
          def encodeNonNull(data: Instant, configuration: ValueCodecConfiguration): Value =
            DateTimeValue(data.toEpochMilli, Int64Millis)
        }
    }

    object Micros extends base(LogicalTypes.TimestampMicrosType) {

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

      implicit val instantEncoder: OptionalValueEncoder[Instant] =
        new OptionalValueEncoder[Instant] {
          def encodeNonNull(data: Instant, configuration: ValueCodecConfiguration): Value = {
            val micros = (data.getEpochSecond * MicrosPerSecond) + (data.getNano / NanosPerMicro)
            DateTimeValue(micros, Int64Micros)
          }
        }
    }

    object Nanos extends base(LogicalTypes.TimestampNanosType) {

      implicit override val sqlTimestampEncoder: OptionalValueEncoder[Timestamp] = new OptionalValueEncoder[Timestamp] {
        def encodeNonNull(data: Timestamp, configuration: ValueCodecConfiguration): Value = {
          val instant = data.toInstant
          val nanos   = (instant.getEpochSecond * MicrosPerSecond * NanosPerMicro) + instant.getNano
          DateTimeValue(nanos, Int64Nanos)
        }
      }

      implicit override val localDateTimeEncoder: OptionalValueEncoder[LocalDateTime] =
        new OptionalValueEncoder[LocalDateTime] {
          def encodeNonNull(data: LocalDateTime, configuration: ValueCodecConfiguration): Value = {
            val zoned = ZonedDateTime.of(data, configuration.timeZone.toZoneId)
            val nanos = (zoned.toEpochSecond * MicrosPerSecond * NanosPerMicro) + zoned.getNano
            DateTimeValue(nanos, Int64Nanos)
          }
        }

      implicit override val instantEncoder: OptionalValueEncoder[Instant] = new OptionalValueEncoder[Instant] {
        def encodeNonNull(data: Instant, configuration: ValueCodecConfiguration): Value = {
          val nanos = (data.getEpochSecond * MicrosPerSecond * NanosPerMicro) + data.getNano
          DateTimeValue(nanos, Int64Nanos)
        }
      }
    }
  }

}
