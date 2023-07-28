package com.github.mjakubowski84.parquet4s

import shapeless.LowPriority

import scala.annotation.nowarn

trait ProductEncoders {
  implicit def productEncoder[T](implicit
      @nowarn ev: LowPriority,
      encoder: ParquetRecordEncoder[T]
  ): OptionalValueEncoder[T] =
    (data, configuration) => encoder.encode(data, null, configuration)
}
