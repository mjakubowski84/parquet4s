package com.github.mjakubowski84.parquet4s

import java.util.TimeZone

/** Configuration necessary for some of codecs
  *
  * @param timeZone
  *   used when encoding and decoding time-based values
  */
case class ValueCodecConfiguration(timeZone: TimeZone)

object ValueCodecConfiguration {
  val Default: ValueCodecConfiguration = ValueCodecConfiguration(timeZone = TimeZone.getDefault)

  def apply(readerOptions: ParquetReader.Options): ValueCodecConfiguration =
    ValueCodecConfiguration(readerOptions.timeZone)

  def apply(writerOptions: ParquetWriter.Options): ValueCodecConfiguration =
    ValueCodecConfiguration(writerOptions.timeZone)

}
