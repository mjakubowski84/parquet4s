package com.github.mjakubowski84.parquet4s

import com.github.mjakubowski84.parquet4s.CompatibilityTestCases.TimePrimitives

class TimeEncodingInt64MillisCompatibilityItSpec extends TimeEncodingCompatibilityItSpec {

  import TimestampFormat.Implicits.Millis.*

  override val outputTimestampType: OutputTimestampType                       = TIMESTAMP_MILLIS
  override protected val parquetWriter: ParquetWriter.Builder[TimePrimitives] = ParquetWriter.of[TimePrimitives]

}
