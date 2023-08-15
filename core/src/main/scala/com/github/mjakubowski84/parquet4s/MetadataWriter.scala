package com.github.mjakubowski84.parquet4s

private[parquet4s] object MetadataWriter {
  val NoOp: MetadataWriter = () => Map.empty
}

private[parquet4s] trait MetadataWriter {
  def getMetadata(): Map[String, String]
}

private[parquet4s] object MetadataReader {
  val NoOp: MetadataReader = _ => {}
}
private[parquet4s] trait MetadataReader {
  def setMetadata(metadata: collection.Map[String, String]): Unit
}
