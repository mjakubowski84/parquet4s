package com.github.mjakubowski84.parquet4s

import java.time.{LocalDate, LocalDateTime}

/**
 * Provides simple conversion methods for primitives.
 */
object ValueImplicits {
  
  import ValueCodecConfiguration._
  
  implicit class IntWrapper(v: Int)(implicit codec: ValueCodec[Int]) {
    def value: Value = codec.encode(v, default)
  }
  implicit class LongWrapper(v: Long)(implicit codec: ValueCodec[Long]) {
    def value: Value = codec.encode(v, default)
  }
  implicit class FloatWrapper(v: Float)(implicit codec: ValueCodec[Float]) {
    def value: Value = codec.encode(v, default)
  }
  implicit class DoubleWrapper(v: Double)(implicit codec: ValueCodec[Double]) {
    def value: Value = codec.encode(v, default)
  }
  implicit class ByteWrapper(v: Byte)(implicit codec: ValueCodec[Byte]) {
    def value: Value = codec.encode(v, default)
  }
  implicit class ShortWrapper(v: Short)(implicit codec: ValueCodec[Short]) {
    def value: Value = codec.encode(v, default)
  }
  implicit class BooleanWrapper(v: Boolean)(implicit codec: ValueCodec[Boolean]) {
    def value: Value = codec.encode(v, default)
  }
  implicit class StringWrapper(v: String)(implicit codec: ValueCodec[String]) {
    def value: Value = codec.encode(v, default)
  }
  implicit class CharWrapper(v: Char)(implicit codec: ValueCodec[Char]) {
    def value: Value = codec.encode(v, default)
  }
  implicit class BigDecimalWrapper(v: BigDecimal)(implicit codec: ValueCodec[BigDecimal]) {
    def value: Value = codec.encode(v, default)
  }
  implicit class LocalDateTimeWrapper(v: LocalDateTime)(implicit codec: ValueCodec[LocalDateTime]) {
    def value: Value = codec.encode(v, default)
  }
  implicit class LocalDateWrapper(v: LocalDate)(implicit codec: ValueCodec[LocalDate]) {
    def value: Value = codec.encode(v, default)
  }
  implicit class TimestampWrapper(v: java.sql.Timestamp)(implicit codec: ValueCodec[java.sql.Timestamp]) {
    def value: Value = codec.encode(v, default)
  }
  implicit class DateWrapper(v: java.sql.Date)(implicit codec: ValueCodec[java.sql.Date]) {
    def value: Value = codec.encode(v, default)
  }
  
}
