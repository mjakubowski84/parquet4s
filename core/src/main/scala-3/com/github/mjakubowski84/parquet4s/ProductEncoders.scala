package com.github.mjakubowski84.parquet4s

import scala.util.NotGiven

trait ProductEncoders:

  given productEncoder[T](using
      ev: NotGiven[ValueEncoder[T]],
      encoder: ParquetRecordEncoder[T]
  ): OptionalValueEncoder[T] =
    (data, configuration) => encoder.encode(data, null, configuration)

end ProductEncoders
