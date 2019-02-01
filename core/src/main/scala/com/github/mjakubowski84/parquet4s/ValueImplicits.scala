package com.github.mjakubowski84.parquet4s

import scala.language.implicitConversions

/**
  * Auxiliary conversions to write more concise code, e.g. when initializing [[RowParquetRecord]].
  */
object ValueImplicits extends AllValueCodecs {

  implicit def valueConversion[T](value: T)(implicit valueCodec: ValueCodec[T]): Value = valueCodec.encode(value)

  implicit def leftTupleConversion[A](tuple: (A, Value))(implicit
                                                         valueACodec: ValueCodec[A]
                                                        ): (Value, Value) = (valueACodec.encode(tuple._1), tuple._2)
  implicit def rightTupleConversion[B](tuple: (Value, B))(implicit
                                                          valueBCodec: ValueCodec[B]
                                                         ): (Value, Value) = (tuple._1, valueBCodec.encode(tuple._2))

  implicit def tupleConversion[A, B](tuple: (A, B))(implicit
                                                    valueACodec: ValueCodec[A],
                                                    valueBCodec: ValueCodec[B]
                                                    ): (Value, Value) = (valueACodec.encode(tuple._1), valueBCodec.encode(tuple._2))

}
