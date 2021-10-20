package com.github.mjakubowski84.parquet4s

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class ValueCodecSpec extends AnyFlatSpec with Matchers {

  case class TestType(i: Int)

  val requiredValueCodec: RequiredValueCodec[TestType] = new RequiredValueCodec[TestType] {
    override protected def decodeNonNull(value: Value, configuration: ValueCodecConfiguration): TestType = value match {
      case IntValue(i) => TestType(i)
    }
    override protected def encodeNonNull(data: TestType, configuration: ValueCodecConfiguration): Value = IntValue(
      data.i
    )
  }
  val optionalValueCodec: OptionalValueCodec[TestType] = new OptionalValueCodec[TestType] {
    override protected def decodeNonNull(value: Value, configuration: ValueCodecConfiguration): TestType = value match {
      case IntValue(i) => TestType(i)
    }
    override protected def encodeNonNull(data: TestType, configuration: ValueCodecConfiguration): Value = IntValue(
      data.i
    )
  }

  val testType: TestType                     = TestType(42)
  val testValue: IntValue                    = IntValue(testType.i)
  val configuration: ValueCodecConfiguration = ValueCodecConfiguration.default

  "Required value codec" should "encode non-null value" in {
    requiredValueCodec.encode(testType, configuration) should be(testValue)
  }

  it should "decode non-null value" in {
    requiredValueCodec.decode(testValue, configuration) should be(testType)
  }

  it should "throw an exception when decoding null-value" in {
    an[IllegalArgumentException] should be thrownBy requiredValueCodec.decode(NullValue, configuration)
  }

  it should "throw an exception when encoding null" in {
    an[IllegalArgumentException] should be thrownBy requiredValueCodec.encode(
      null.asInstanceOf[TestType],
      configuration
    )
  }

  "Optional value codec" should "encode non-null value" in {
    optionalValueCodec.encode(testType, configuration) should be(testValue)
  }

  it should "decode non-null value" in {
    optionalValueCodec.decode(testValue, configuration) should be(testType)
  }

  it should "throw an exception when decoding null-value" in {
    optionalValueCodec.decode(NullValue, configuration) should be(null)
  }

  it should "throw an exception when encoding null" in {
    optionalValueCodec.encode(null.asInstanceOf[TestType], configuration) should be(NullValue)
  }

}
