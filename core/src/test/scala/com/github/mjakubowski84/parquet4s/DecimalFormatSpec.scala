package com.github.mjakubowski84.parquet4s

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import java.math.MathContext

class DecimalFormatSpec extends AnyFlatSpec with Matchers {

  private val vcc = ValueCodecConfiguration.Default

  "BinaryFormat" should "be able to convert decimal to binary and back" in {
    val format          = DecimalFormat.Default
    val decimal         = BigDecimal(1001, 2)
    val value           = format.Implicits.decimalEncoder.encode(decimal, vcc)
    val revertedDecimal = format.Implicits.decimalDecoder.decode(value, vcc)

    revertedDecimal should be(decimal)
  }

  it should "be able to convert negative decimal to binary and back" in {
    val format          = DecimalFormat.Default
    val decimal         = BigDecimal(-1001, 2)
    val value           = format.Implicits.decimalEncoder.encode(decimal, vcc)
    val revertedDecimal = format.Implicits.decimalDecoder.decode(value, vcc)

    revertedDecimal should be(decimal)
  }

  it should "be able to convert zero decimal to binary and back" in {
    val format          = DecimalFormat.Default
    val decimal         = BigDecimal(0, 0)
    val value           = format.Implicits.decimalEncoder.encode(decimal, vcc)
    val revertedDecimal = format.Implicits.decimalDecoder.decode(value, vcc)

    revertedDecimal should be(decimal)
  }

  it should "handle decimal with scale lower than Parquet4S uses by default" in {
    val format  = DecimalFormat.binaryFormat(scale = 2, precision = 4, byteArrayLength = 16, rescaleOnRead = true)
    val decimal = BigDecimal(1L, 1, new MathContext(3))
    val value   = format.Implicits.decimalEncoder.encode(decimal, vcc)
    val revertedDecimal = format.Implicits.decimalDecoder.decode(value, vcc)

    revertedDecimal should be(decimal)
  }

  it should "round decimal with scale greater than Parquet4S uses" in {
    val format  = DecimalFormat.binaryFormat(scale = 2, precision = 4, byteArrayLength = 16, rescaleOnRead = true)
    val decimal = BigDecimal(1L, 4, new MathContext(8))
    val value   = format.Implicits.decimalEncoder.encode(decimal, vcc)
    val revertedDecimal = format.Implicits.decimalDecoder.decode(value, vcc)

    revertedDecimal should be(0)
  }

  "IntFormat" should "be able to convert decimal to binary and back" in {
    val format          = DecimalFormat.intFormat(scale = 2, precision = 8, rescaleOnRead = false)
    val decimal         = BigDecimal(1001, 2)
    val value           = format.Implicits.decimalEncoder.encode(decimal, vcc)
    val revertedDecimal = format.Implicits.decimalDecoder.decode(value, vcc)

    revertedDecimal should be(decimal)
  }

  "LongFormat" should "be able to convert decimal to binary and back" in {
    val format          = DecimalFormat.longFormat(scale = 2, precision = 8, rescaleOnRead = false)
    val decimal         = BigDecimal(1001, 2)
    val value           = format.Implicits.decimalEncoder.encode(decimal, vcc)
    val revertedDecimal = format.Implicits.decimalDecoder.decode(value, vcc)

    revertedDecimal should be(decimal)
  }

}
