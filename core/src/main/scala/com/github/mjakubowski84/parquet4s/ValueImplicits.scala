package com.github.mjakubowski84.parquet4s

import scala.language.implicitConversions

/**
  * Auxiliary conversions to write more concise code, e.g. when initializing [[RowParquetRecord]].
  */
object ValueImplicits extends AllValueCodecs {

  implicit val valueCodecConfiguration: ValueCodecConfiguration = ValueCodecConfiguration.default

  implicit def valueConversion[T](value: T)(implicit
                                            valueCodec: ValueCodec[T],
                                            configuration: ValueCodecConfiguration
                                            ): Value = valueCodec.encode(value, configuration)

  implicit def leftTupleConversion[A](tuple: (A, Value))(implicit
                                                         valueACodec: ValueCodec[A],
                                                         configuration: ValueCodecConfiguration
                                                        ): (Value, Value) =
    (valueACodec.encode(tuple._1, configuration), tuple._2)
  implicit def rightTupleConversion[B](tuple: (Value, B))(implicit
                                                          valueBCodec: ValueCodec[B],
                                                          configuration: ValueCodecConfiguration
                                                         ): (Value, Value) =
    (tuple._1, valueBCodec.encode(tuple._2, configuration))

  implicit def tupleConversion[A, B](tuple: (A, B))(implicit
                                                    valueACodec: ValueCodec[A],
                                                    valueBCodec: ValueCodec[B],
                                                    configuration: ValueCodecConfiguration
                                                    ): (Value, Value) =
    (valueACodec.encode(tuple._1, configuration), valueBCodec.encode(tuple._2, configuration))

}
