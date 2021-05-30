package com.github.mjakubowski84.parquet4s

import scala.annotation.implicitNotFound

trait AllValueEncoders
  extends PrimitiveValueEncoders
    with TimeValueEncoders
    with ComplexValueEncoders

/**
 * Contains implicit instances of all [[ValueEncoder]]
 */
object ValueEncoder extends AllValueEncoders

@implicitNotFound("Missing ValueEncoder for value type ${T}. Implement your own encoder in order to serialise your data.")
trait ValueEncoder[T] {
  /**
   * @param data          source data
   * @param configuration [ValueCodecConfiguration] used by some codecs
   * @return encoded Parquet [[Value]]
   */
  def encode(data: T, configuration: ValueCodecConfiguration): Value
}

/**
 * Encoder for non-null type of [[Value]]
 * @tparam T data type to encode to
 */
trait RequiredValueEncoder[T] extends ValueEncoder[T] {
  override def encode(data: T, configuration: ValueCodecConfiguration): Value =
    Option(data) match {
      case None =>
        throw new IllegalArgumentException("Cannot encode null instance of required type")
      case Some(other) =>
        encodeNonNull(other, configuration)
    }

  protected def encodeNonNull(data: T, configuration: ValueCodecConfiguration): Value
}


/**
 * Encoder for [[Value]] that can be null.
 * @tparam T data type to encode to
 */
trait OptionalValueEncoder[T] extends ValueEncoder[T] {
  override def encode(data: T, configuration: ValueCodecConfiguration): Value =
    Option(data).fold[Value](NullValue)(nonNullData => encodeNonNull(nonNullData, configuration))

  protected def encodeNonNull(data: T, configuration: ValueCodecConfiguration): Value
}
