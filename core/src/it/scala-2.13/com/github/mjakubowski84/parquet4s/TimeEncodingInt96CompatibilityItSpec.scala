package com.github.mjakubowski84.parquet4s

class TimeEncodingInt96MillisCompatibilityItSpec extends TimeEncodingCompatibilityItSpec {

  override val outputTimestampType: OutputTimestampType = Int96

}
