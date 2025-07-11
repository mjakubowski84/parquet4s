package com.github.mjakubowski84.parquet4s

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.Inspectors

import java.sql.Timestamp
import java.time.{Instant, LocalDateTime}
import java.util.TimeZone

class TimestampFormatSpec extends AnyFlatSpec with Matchers with Inspectors {
  private val defaultConfiguration = ValueCodecConfiguration(TimeZone.getTimeZone("Africa/Nairobi"))

  private val timeZones = Seq("GMT-1:00", "UTC", "GMT+1:00").map(TimeZone.getTimeZone)

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

  it should "decode previously saved instant as other time type" in {
    val instantEncoder       = TimestampFormat.Implicits.Millis.instantEncoder
    val timestampDecoder     = implicitly[ValueDecoder[Timestamp]]
    val localDateTimeDecoder = implicitly[ValueDecoder[LocalDateTime]]

    val instant = Instant.parse("2024-01-01T12:00:00.101Z")

    timeZones.foreach { timeZone =>
      val vcc = defaultConfiguration.copy(timeZone = timeZone)

      val dateTimeValue        = instantEncoder.encode(instant, vcc)
      val decodedTimestamp     = timestampDecoder.decode(dateTimeValue, vcc)
      val decodedLocalDateTime = localDateTimeDecoder.decode(dateTimeValue, vcc)

      decodedTimestamp should be(Timestamp.from(instant))
      decodedLocalDateTime should be(LocalDateTime.ofInstant(instant, vcc.timeZone.toZoneId))
    }
  }

  it should "decode instant from other encoded types" in {
    val instantDecoder       = implicitly[ValueDecoder[Instant]]
    val timestampEncoder     = TimestampFormat.Implicits.Millis.sqlTimestampEncoder
    val localDateTimeEncoder = TimestampFormat.Implicits.Millis.localDateTimeEncoder

    val instant = Instant.parse("2024-01-01T12:00:00.101Z")

    timeZones.foreach { timeZone =>
      val vcc = defaultConfiguration.copy(timeZone = timeZone)

      val encodedTimestamp = timestampEncoder.encode(Timestamp.from(instant), vcc)
      val encodedLocalDateTime =
        localDateTimeEncoder.encode(LocalDateTime.ofInstant(instant, vcc.timeZone.toZoneId), vcc)

      instantDecoder.decode(encodedTimestamp, vcc) should be(instant)
      instantDecoder.decode(encodedLocalDateTime, vcc) should be(instant)
    }
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
