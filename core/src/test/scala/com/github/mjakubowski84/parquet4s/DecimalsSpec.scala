package com.github.mjakubowski84.parquet4s

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers


class DecimalsSpec extends AnyFlatSpec with Matchers {

  "Decimals" should "be able to convert decimal to binary and back" in {
    val decimal = BigDecimal(1001, 2)
    val binary = Decimals.binaryFromDecimal(decimal)
    val revertedDecimal = Decimals.decimalFromBinary(binary)

    revertedDecimal should be(decimal)
  }

  it should "be able to convert negative decimal to binary and back" in {
    val decimal = BigDecimal(-1001, 2)
    val binary = Decimals.binaryFromDecimal(decimal)
    val revertedDecimal = Decimals.decimalFromBinary(binary)

    revertedDecimal should be(decimal)
  }

  it should "be able to convert zero decimal to binary and back" in {
    val decimal = BigDecimal(0, 0)
    val binary = Decimals.binaryFromDecimal(decimal)
    val revertedDecimal = Decimals.decimalFromBinary(binary)

    revertedDecimal should be(decimal)
  }

  it should "round decimal with scale greater than Parquet4S uses" in {
    val decimal = BigDecimal(1L, Decimals.Scale + 1, Decimals.MathContext)
    val binary = Decimals.binaryFromDecimal(decimal)
    val revertedDecimal = Decimals.decimalFromBinary(binary)

    revertedDecimal should be(0)
  }

}
