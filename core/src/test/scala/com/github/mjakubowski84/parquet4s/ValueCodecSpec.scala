package com.github.mjakubowski84.parquet4s

import org.scalatest.{FlatSpec, Matchers}


class ValueCodecSpec extends FlatSpec with Matchers {

  case class TestType(text: String)

  val requiredValueCodec: RequiredValueCodec[TestType] = new RequiredValueCodec[TestType] {
    override protected def decodeNonNull(value: Value, configuration: ValueCodecConfiguration): TestType = value match {
      case StringValue(string) => TestType(string)
    }
    override protected def encodeNonNull(data: TestType, configuration: ValueCodecConfiguration): Value = StringValue(data.text)
  }
  val optionalValueCodec: OptionalValueCodec[TestType] = new OptionalValueCodec[TestType] {
    override protected def decodeNonNull(value: Value, configuration: ValueCodecConfiguration): TestType = value match {
      case StringValue(string) => TestType(string)
    }
    override protected def encodeNonNull(data: TestType, configuration: ValueCodecConfiguration): Value = StringValue(data.text)
  }

  val text = TestType("text")
  val textValue = StringValue(text.text)
  val configuration: ValueCodecConfiguration = ValueCodecConfiguration.default

  "Required value codec" should "encode non-null value" in {
    requiredValueCodec.encode(text, configuration) should be(textValue)
  }

  it should "decode non-null value" in {
    requiredValueCodec.decode(textValue, configuration) should be(text)
  }

  it should "throw an exception when decoding null-value" in {
    an[IllegalArgumentException] should be thrownBy requiredValueCodec.decode(NullValue, configuration)
  }

  it should "throw an exception when encoding null" in {
    an[IllegalArgumentException] should be thrownBy requiredValueCodec.encode(null.asInstanceOf[TestType], configuration)
  }

  "Optional value codec" should "encode non-null value" in {
    optionalValueCodec.encode(text, configuration) should be(textValue)
  }

  it should "decode non-null value" in {
    optionalValueCodec.decode(textValue, configuration) should be(text)
  }

  it should "throw an exception when decoding null-value" in {
    optionalValueCodec.decode(NullValue, configuration) should be(null)
  }

  it should "throw an exception when encoding null" in {
    optionalValueCodec.encode(null.asInstanceOf[TestType], configuration) should be(NullValue)
  }

}
