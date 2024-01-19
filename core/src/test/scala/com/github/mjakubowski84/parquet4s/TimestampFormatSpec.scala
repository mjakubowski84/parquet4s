package com.github.mjakubowski84.parquet4s

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.sql.Timestamp
import java.time.{Instant, LocalDateTime}
import java.util.TimeZone

class TimestampFormatSpec extends AnyFlatSpec with Matchers {
  private val defaultConfiguration = ValueCodecConfiguration(TimeZone.getTimeZone("Africa/Nairobi"))

  behavior of "TimestampFormat.Implicits.Millis"

  it should "be able to encode Instant and decode it back" in {
    import TimestampFormat.Implicits.Millis.*
    val instant        = Instant.ofEpochMilli(1234567L)
    val decodedInstant = codec(instant)
    decodedInstant should be(instant)
  }

  it should "be able to encode LocalDateTime and decode it back" in {
    import TimestampFormat.Implicits.Millis.*
    val dateTime        = LocalDateTime.of(2000, 1, 2, 3, 4, 5, 6000000)
    val decodedDateTime = codec(dateTime)
    decodedDateTime should be(dateTime)
  }

  it should "be able to encode Timestamp and decode it back" in {
    import TimestampFormat.Implicits.Millis.*
    val timestamp        = Timestamp.from(Instant.ofEpochMilli(1234567L))
    val decodedTimestamp = codec(timestamp)
    decodedTimestamp should be(timestamp)
  }

  behavior of "TimestampFormat.Implicits.Micros"

  it should "be able to encode Instant and decode it back" in {
    import TimestampFormat.Implicits.Micros.*
    val instant        = Instant.ofEpochSecond(1234567L, 123000L)
    val decodedInstant = codec(instant)
    decodedInstant should be(instant)
  }

  it should "be able to encode LocalDateTime and decode it back" in {
    import TimestampFormat.Implicits.Micros.*
    val dateTime        = LocalDateTime.of(2000, 1, 2, 3, 4, 5, 678000)
    val decodedDateTime = codec(dateTime)
    decodedDateTime should be(dateTime)
  }

  it should "be able to encode Timestamp and decode it back" in {
    import TimestampFormat.Implicits.Micros.*
    val timestamp        = Timestamp.from(Instant.ofEpochSecond(1234567L, 123000L))
    val decodedTimestamp = codec(timestamp)
    decodedTimestamp should be(timestamp)
  }

  behavior of "TimestampFormat.Implicits.Nanos"

  it should "be able to encode Instant and decode it back" in {
    import TimestampFormat.Implicits.Nanos.*
    val instant        = Instant.ofEpochSecond(1234567L, 1234567L)
    val decodedInstant = codec(instant)
    decodedInstant should be(instant)
  }

  it should "be able to encode LocalDateTime and decode it back" in {
    import TimestampFormat.Implicits.Nanos.*
    val dateTime        = LocalDateTime.of(2000, 1, 2, 3, 4, 5, 6789)
    val decodedDateTime = codec(dateTime)
    decodedDateTime should be(dateTime)
  }

  it should "be able to encode Timestamp and decode it back" in {
    import TimestampFormat.Implicits.Nanos.*
    val timestamp        = Timestamp.from(Instant.ofEpochSecond(1234567L, 1234567L))
    val decodedTimestamp = codec(timestamp)
    decodedTimestamp should be(timestamp)
  }

  private def codec[A](a: A)(implicit encoder: ValueEncoder[A], decoder: ValueDecoder[A]): A = {
    val value = encoder.encode(a, defaultConfiguration)
    decoder.decode(value, defaultConfiguration)
  }
}
