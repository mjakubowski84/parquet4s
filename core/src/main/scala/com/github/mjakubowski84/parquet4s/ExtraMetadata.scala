package com.github.mjakubowski84.parquet4s

private[parquet4s] object ExtraMetadata {
  def NoExtraMetadata: ExtraMetadata = () => Map.empty
}

private[parquet4s] trait ExtraMetadata {
  def getExtraMetadata(): Map[String, String]
}
