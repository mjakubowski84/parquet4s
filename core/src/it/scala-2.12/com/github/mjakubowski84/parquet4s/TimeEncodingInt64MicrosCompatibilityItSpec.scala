package com.github.mjakubowski84.parquet4s

import com.github.mjakubowski84.parquet4s.CompatibilityTestCases.TimePrimitives

class TimeEncodingInt64MicrosCompatibilityItSpec extends TimeEncodingCompatibilityItSpec {

  import TimestampFormat.Implicits.Micros.*

  override val outputTimestampType: OutputTimestampType                       = Int96
  override protected val parquetWriter: ParquetWriter.Builder[TimePrimitives] = ParquetWriter.of[TimePrimitives]

}
