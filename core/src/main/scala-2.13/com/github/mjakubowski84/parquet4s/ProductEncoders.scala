package com.github.mjakubowski84.parquet4s

import shapeless.LowPriority

trait ProductEncoders {
  implicit def productEncoder[T](implicit
      ev: LowPriority,
      encoder: ParquetRecordEncoder[T]
  ): OptionalValueEncoder[T] =
    (data, configuration) => encoder.encode(data, null, configuration)
}
