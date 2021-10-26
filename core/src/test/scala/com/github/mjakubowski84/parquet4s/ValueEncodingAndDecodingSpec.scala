package com.github.mjakubowski84.parquet4s

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class ValueEncodingAndDecodingSpec extends AnyFlatSpec with Matchers {

  case class TestType(i: Int)

  val requiredValueEncoder: RequiredValueEncoder[TestType] = (data, _) => IntValue(data.i)
  val requiredValueDecoder: RequiredValueDecoder[TestType] = (value, _) =>
    value match {
      case IntValue(i) => TestType(i)
    }
  val optionalValueEncoder: OptionalValueEncoder[TestType] = (data, _) => IntValue(data.i)
  val optionalValueDecoder: OptionalValueDecoder[TestType] = (value, _) =>
    value match {
      case IntValue(i) => TestType(i)
    }

  val testType: TestType                     = TestType(42)
  val testValue: IntValue                    = IntValue(testType.i)
  val configuration: ValueCodecConfiguration = ValueCodecConfiguration.Default

  "Required value encoder" should "encode non-null value" in {
    requiredValueEncoder.encode(testType, configuration) should be(testValue)
  }

  it should "throw an exception when encoding null" in {
    an[IllegalArgumentException] should be thrownBy requiredValueEncoder.encode(
      null.asInstanceOf[TestType],
      configuration
    )
  }

  "Required value decoder" should "decode non-null value" in {
    requiredValueDecoder.decode(testValue, configuration) should be(testType)
  }

  it should "throw an exception when decoding null-value" in {
    an[IllegalArgumentException] should be thrownBy requiredValueDecoder.decode(NullValue, configuration)
  }

  "Optional value encoder" should "encode non-null value" in {
    optionalValueEncoder.encode(testType, configuration) should be(testValue)
  }

  it should "throw an exception when encoding null" in {
    optionalValueEncoder.encode(null.asInstanceOf[TestType], configuration) should be(NullValue)
  }

  "Optional value decoder" should "throw an exception when decoding null-value" in {
    optionalValueDecoder.decode(NullValue, configuration) should be(null)
  }

  it should "decode non-null value" in {
    optionalValueDecoder.decode(testValue, configuration) should be(testType)
  }

}
