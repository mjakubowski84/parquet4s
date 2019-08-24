package com.github.mjakubowski84.parquet4s

import java.math.MathContext
import java.nio.ByteBuffer

import org.apache.parquet.io.api.Binary

object Decimals {
  val Scale = 18
  val Precision = 38
  val ByteArrayLength = 16
  val MathContext = new MathContext(Precision)

  private def rescale(original: BigDecimal): BigDecimal = {
    if (original.scale == Scale && original.mc == MathContext) original
    else BigDecimal.decimal(original.bigDecimal, MathContext).setScale(Scale, BigDecimal.RoundingMode.HALF_UP)
  }

  def rescaleBinary(binary: Binary, originalScale: Int, originalMathContext: MathContext): Binary =
    binaryFromDecimal(decimalFromBinary(binary, originalScale, originalMathContext))

  def decimalFromBinary(binary: Binary, scale: Int = Scale, mathContext: MathContext = MathContext): BigDecimal =
    BigDecimal(BigInt(binary.getBytes), scale, mathContext)

  def binaryFromDecimal(decimal: BigDecimal): Binary = {
    /*
      Decimal is stored as byte array of unscaled BigInteger.
      Scale and precision is stored separately in metadata.
      Value needs to be rescaled with default scale and precision for BigDecimal before saving.
    */
    val buf = ByteBuffer.allocate(ByteArrayLength)
    val unscaled = rescale(decimal).bigDecimal.unscaledValue().toByteArray
    // BigInteger is stored in tail of byte array, sign is stored in unoccupied cells
    val sign: Byte = if (unscaled.head < 0) -1 else 0
    (0 until ByteArrayLength - unscaled.length).foreach(_ => buf.put(sign))
    buf.put(unscaled)
    Binary.fromReusedByteArray(buf.array())
  }
}
