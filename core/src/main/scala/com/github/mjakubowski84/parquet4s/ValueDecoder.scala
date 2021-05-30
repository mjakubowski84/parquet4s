package com.github.mjakubowski84.parquet4s

import scala.annotation.implicitNotFound


trait AllValueDecoders
  extends PrimitiveValueDecoders
    with TimeValueDecoders
    with ComplexValueDecoders

/**
 * Contains implicit instances of all [[ValueDecoder]]
 */
object ValueDecoder extends AllValueDecoders

@implicitNotFound("Missing ValueDecoder for value type ${T}. Implement your own decoder in order to deserialise your data.")
trait ValueDecoder[T] {
  /**
   * @param value source Parquet [[Value]]
   * @param configuration [ValueCodecConfiguration] used by some codecs
   * @return data decoded from [[Value]]
   */
  def decode(value: Value, configuration: ValueCodecConfiguration): T
}

/**
  * Decoder for non-null type of [[Value]]
  * @tparam T data type to decode from
  */
trait RequiredValueDecoder[T] extends ValueDecoder[T] {

  final override def decode(value: Value, configuration: ValueCodecConfiguration): T =
    value match {
      case NullValue =>
        throw new IllegalArgumentException("NullValue cannot be decoded to required type")
      case other =>
        decodeNonNull(other, configuration)
    }

  protected def decodeNonNull(value: Value, configuration: ValueCodecConfiguration): T

}

/**
  * Decoder for [[Value]] that can be null.
  * @tparam T data type to decode from
  */
trait OptionalValueDecoder[T] extends ValueDecoder[T] {

  final override def decode(value: Value, configuration: ValueCodecConfiguration): T =
    value match {
      case NullValue => null.asInstanceOf[T]
      case other => decodeNonNull(other, configuration)
    }

  protected def decodeNonNull(value: Value, configuration: ValueCodecConfiguration): T

}
