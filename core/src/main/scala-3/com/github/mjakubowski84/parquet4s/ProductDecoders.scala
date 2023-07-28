package com.github.mjakubowski84.parquet4s

import scala.util.NotGiven

trait ProductDecoders:

  given productDecoder[T](using
      ev: NotGiven[ValueDecoder[T]],
      decoder: ParquetRecordDecoder[T]
  ): OptionalValueDecoder[T] =
    (value, configuration) =>
      value match
        case record: RowParquetRecord => decoder.decode(record, configuration)

end ProductDecoders
