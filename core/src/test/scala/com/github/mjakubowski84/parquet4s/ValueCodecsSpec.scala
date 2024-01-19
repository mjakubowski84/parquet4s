package com.github.mjakubowski84.parquet4s

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.time.Instant
import java.util.TimeZone

class ValueCodecsSpec extends AnyFlatSpec with Matchers {
  private val defaultConfiguration = ValueCodecConfiguration(TimeZone.getTimeZone("Africa/Nairobi"))

  behavior of "Default timestamp format (INT96)"

  it should "be able to encode Instant and decode it back" in {
    val instant        = Instant.ofEpochMilli(1234567L)
    val decodedInstant = codec(instant)
    decodedInstant should be(instant)
  }

  private def codec[A](a: A)(implicit encoder: ValueEncoder[A], decoder: ValueDecoder[A]): A = {
    val value = encoder.encode(a, defaultConfiguration)
    decoder.decode(value, defaultConfiguration)
  }
}
