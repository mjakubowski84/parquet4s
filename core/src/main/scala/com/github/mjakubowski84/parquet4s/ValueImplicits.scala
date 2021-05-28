package com.github.mjakubowski84.parquet4s

import java.time.{LocalDate, LocalDateTime}

/**
 * Provides simple conversion methods for primitives.
 */
object ValueImplicits {
  
  import ValueCodecConfiguration._
  
  implicit class IntWrapper(v: Int)(implicit encoder: ValueEncoder[Int]) {
    def value: Value = encoder.encode(v, Default)
  }
  implicit class LongWrapper(v: Long)(implicit encoder: ValueEncoder[Long]) {
    def value: Value = encoder.encode(v, Default)
  }
  implicit class FloatWrapper(v: Float)(implicit encoder: ValueEncoder[Float]) {
    def value: Value = encoder.encode(v, Default)
  }
  implicit class DoubleWrapper(v: Double)(implicit encoder: ValueEncoder[Double]) {
    def value: Value = encoder.encode(v, Default)
  }
  implicit class ByteWrapper(v: Byte)(implicit encoder: ValueEncoder[Byte]) {
    def value: Value = encoder.encode(v, Default)
  }
  implicit class ShortWrapper(v: Short)(implicit encoder: ValueEncoder[Short]) {
    def value: Value = encoder.encode(v, Default)
  }
  implicit class BooleanWrapper(v: Boolean)(implicit encoder: ValueEncoder[Boolean]) {
    def value: Value = encoder.encode(v, Default)
  }
  implicit class StringWrapper(v: String)(implicit encoder: ValueEncoder[String]) {
    def value: Value = encoder.encode(v, Default)
  }
  implicit class CharWrapper(v: Char)(implicit encoder: ValueEncoder[Char]) {
    def value: Value = encoder.encode(v, Default)
  }
  implicit class BigDecimalWrapper(v: BigDecimal)(implicit encoder: ValueEncoder[BigDecimal]) {
    def value: Value = encoder.encode(v, Default)
  }
  implicit class LocalDateTimeWrapper(v: LocalDateTime)(implicit encoder: ValueEncoder[LocalDateTime]) {
    def value: Value = encoder.encode(v, Default)
  }
  implicit class LocalDateWrapper(v: LocalDate)(implicit encoder: ValueEncoder[LocalDate]) {
    def value: Value = encoder.encode(v, Default)
  }
  implicit class TimestampWrapper(v: java.sql.Timestamp)(implicit encoder: ValueEncoder[java.sql.Timestamp]) {
    def value: Value = encoder.encode(v, Default)
  }
  implicit class DateWrapper(v: java.sql.Date)(implicit encoder: ValueEncoder[java.sql.Date]) {
    def value: Value = encoder.encode(v, Default)
  }
  
}
