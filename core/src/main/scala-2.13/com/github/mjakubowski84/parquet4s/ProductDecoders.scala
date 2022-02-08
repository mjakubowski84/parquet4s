package com.github.mjakubowski84.parquet4s

import shapeless.LowPriority

trait ProductDecoders {

  implicit def productDecoder[T](implicit
      ev: LowPriority,
      decoder: ParquetRecordDecoder[T]
  ): OptionalValueDecoder[T] =
    (value, configuration) =>
      value match {
        case record: RowParquetRecord => decoder.decode(record, configuration)
      }

}
