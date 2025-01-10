package com.github.mjakubowski84.parquet4s

import java.math.MathContext
import java.nio.ByteBuffer
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.*
import org.apache.parquet.filter2.predicate.Operators.*

import org.apache.parquet.io.api.Binary
import java.math.BigInteger
import org.apache.parquet.io.api.RecordConsumer
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName

object DecimalFormat {

  val Default: BinaryFormat & BinaryImplicits = binaryFormat(
    scale           = 18,
    precision       = 38,
    byteArrayLength = 16,
    rescaleOnRead   = true
  )

  def binaryFormat(
      scale: Int,
      precision: Int,
      byteArrayLength: Int,
      rescaleOnRead: Boolean
  ): BinaryFormat & BinaryImplicits =
    new BinaryFormat(scale, precision, byteArrayLength, rescaleOnRead) with BinaryImplicits

  def intFormat(scale: Int, precision: Int, rescaleOnRead: Boolean): NumericFormat & IntImplicits =
    new IntFormat(scale, precision, rescaleOnRead) with IntImplicits

  def longFormat(scale: Int, precision: Int, rescaleOnRead: Boolean): NumericFormat & LongImplicits =
    new LongFormat(scale, precision, rescaleOnRead) with LongImplicits

  trait Format {
    val scale: Int
    val precision: Int
    protected[parquet4s] val mathContext = new MathContext(precision)
    val rescaleOnRead: Boolean

    def write(recordConsumer: RecordConsumer, unscaledValue: BigInteger): Unit

    protected def rescale(original: BigDecimal): BigDecimal =
      if (original.scale == scale && original.mc == mathContext) original
      else BigDecimal.decimal(original.bigDecimal, mathContext).setScale(scale, BigDecimal.RoundingMode.HALF_UP)

  }

  abstract class NumericFormat private[DecimalFormat] (
      val scale: Int,
      val precision: Int,
      val primitiveTypeName: PrimitiveTypeName,
      val rescaleOnRead: Boolean
  ) extends Format

  class IntFormat private[parquet4s] (scale: Int, precision: Int, rescaleOnRead: Boolean)
      extends NumericFormat(scale, precision, PrimitiveTypeName.INT32, rescaleOnRead) {
    override def write(recordConsumer: RecordConsumer, unscaledValue: BigInteger): Unit =
      recordConsumer.addInteger(unscaledValue.intValue())
  }

  class LongFormat private[parquet4s] (scale: Int, precision: Int, rescaleOnRead: Boolean)
      extends NumericFormat(scale, precision, PrimitiveTypeName.INT64, rescaleOnRead) {
    override def write(recordConsumer: RecordConsumer, unscaledValue: BigInteger): Unit =
      recordConsumer.addLong(unscaledValue.longValue())
  }

  class BinaryFormat private[parquet4s] (
      val scale: Int,
      val precision: Int,
      val byteArrayLength: Int,
      val rescaleOnRead: Boolean
  ) extends Format {

    override def write(recordConsumer: RecordConsumer, unscaledValue: BigInteger): Unit =
      recordConsumer.addBinary(bigIntegerToBinary(unscaledValue))

    private[DecimalFormat] def bigIntegerToBinary(unscaledValue: BigInteger): Binary = {
      val buf      = ByteBuffer.allocate(byteArrayLength)
      val unscaled = unscaledValue.toByteArray
      // BigInteger is stored in tail of byte array, sign is stored in unoccupied cells
      val sign: Byte = if (unscaled.head < 0) -1 else 0
      (0 until byteArrayLength - unscaled.length).foreach(_ => buf.put(sign))
      buf.put(unscaled)
      Binary.fromReusedByteArray(buf.array())
    }

    private[DecimalFormat] def bigDecimalFromBinary(binary: Binary): BigDecimal =
      BigDecimal(BigInt(binary.getBytes), scale, mathContext)

    private[parquet4s] def binaryFromDecimal(bigDecimal: BigDecimal): Binary = {
      val unscaled = rescale(bigDecimal).bigDecimal.unscaledValue()
      bigIntegerToBinary(unscaled)
    }

  }

  trait BinaryImplicits extends Implicits[Binary, BinaryColumn] {
    self: BinaryFormat =>

    implicit val decimalFilterEncoderImpl: FilterEncoder[BigDecimal, Binary, BinaryColumn] =
      new FilterEncoder[BigDecimal, Binary, BinaryColumn] {
        val columnFactory: ColumnFactory[Binary, BinaryColumn] = ColumnFactory.binaryColumnFactory

        val encode = (v, _) => {
          val rescaled = rescale(v)
          bigIntegerToBinary(rescaled.bigDecimal.unscaledValue())
        }
      }

    override val decimalFilterDecoderImpl: FilterDecoder[BigDecimal, Binary] =
      new FilterDecoder[BigDecimal, Binary] {
        val decode = (v, _) => bigDecimalFromBinary(v)
      }
  }

  trait IntImplicits extends Implicits[java.lang.Integer, IntColumn] {
    self: IntFormat =>

    override val decimalFilterEncoderImpl: FilterEncoder[BigDecimal, java.lang.Integer, IntColumn] =
      new FilterEncoder[BigDecimal, java.lang.Integer, IntColumn] {
        override val columnFactory: ColumnFactory[java.lang.Integer, IntColumn] = ColumnFactory.intColumnFactory
        val encode = (v, _) => rescale(v).bigDecimal.unscaledValue().intValue
      }

    override val decimalFilterDecoderImpl: FilterDecoder[BigDecimal, java.lang.Integer] =
      new FilterDecoder[BigDecimal, java.lang.Integer] {
        val decode = (v, _) => BigDecimal(v.longValue, scale, mathContext)
      }
  }

  trait LongImplicits extends Implicits[java.lang.Long, LongColumn] {
    self: LongFormat =>

    override val decimalFilterEncoderImpl: FilterEncoder[BigDecimal, java.lang.Long, LongColumn] =
      new FilterEncoder[BigDecimal, java.lang.Long, LongColumn] {
        override val columnFactory: ColumnFactory[java.lang.Long, LongColumn] = ColumnFactory.longColumnFactory
        val encode = (v, _) => rescale(v).bigDecimal.unscaledValue().longValue
      }

    override val decimalFilterDecoderImpl: FilterDecoder[BigDecimal, java.lang.Long] =
      new FilterDecoder[BigDecimal, java.lang.Long] {
        val decode = (v, _) => BigDecimal(v, scale, mathContext)
      }
  }

  trait Implicits[V <: Comparable[V], C <: Column[V]] {
    self: Format =>

    protected val decimalFilterEncoderImpl: FilterEncoder[BigDecimal, V, C]

    protected val decimalFilterDecoderImpl: FilterDecoder[BigDecimal, V]

    object Implicits {

      implicit val decimalEncoder: OptionalValueEncoder[BigDecimal] = new OptionalValueEncoder[BigDecimal] {
        def encodeNonNull(data: BigDecimal, configuration: ValueCodecConfiguration): Value = {
          val rescaled = rescale(data)
          DecimalValue(rescaled.bigDecimal.unscaledValue(), self)
        }
      }

      implicit val decimalDecoder: OptionalValueDecoder[BigDecimal] = new OptionalValueDecoder[BigDecimal] {
        def decodeNonNull(value: Value, configuration: ValueCodecConfiguration): BigDecimal =
          value match {
            case IntValue(int)       => BigDecimal(int, mathContext)
            case LongValue(long)     => BigDecimal.decimal(long, mathContext)
            case DoubleValue(double) => BigDecimal.decimal(double, mathContext)
            case FloatValue(float)   => BigDecimal.decimal(float, mathContext)
            case BinaryValue(binary) => BigDecimal(BigInt(binary.getBytes), scale, mathContext)
            case DecimalValue(unscaled, originalFormat) =>
              val original = BigDecimal(unscaled, originalFormat.scale, originalFormat.mathContext)
              if (rescaleOnRead) rescale(original) else original
          }
      }

      implicit lazy val decimalSchema: TypedSchemaDef[BigDecimal] =
        self match {
          case binaryFormat: BinaryFormat =>
            SchemaDef
              .primitive(
                primitiveType         = FIXED_LEN_BYTE_ARRAY,
                required              = false,
                logicalTypeAnnotation = Option(LogicalTypes.decimalType(scale, precision)),
                length                = Some(binaryFormat.byteArrayLength)
              )
              .withMetadata(SchemaDef.Meta.Generated)
              .typed[BigDecimal]

          case numericFormat: NumericFormat =>
            SchemaDef
              .primitive(
                primitiveType         = numericFormat.primitiveTypeName,
                required              = false,
                logicalTypeAnnotation = Option(LogicalTypes.decimalType(scale, precision))
              )
              .withMetadata(SchemaDef.Meta.Generated)
              .typed[BigDecimal]
        }

      implicit val decimalFilterEncoder: FilterEncoder[BigDecimal, V, C] = decimalFilterEncoderImpl

      implicit val decimalFilterDecoder: FilterDecoder[BigDecimal, V] = decimalFilterDecoderImpl
    }

  }

}
