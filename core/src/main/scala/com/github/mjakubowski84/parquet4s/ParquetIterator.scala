package com.github.mjakubowski84.parquet4s

import java.io.Closeable

private[parquet4s] object ParquetIterator {
  private[parquet4s] type HadoopBuilder[T] = org.apache.parquet.hadoop.ParquetReader.Builder[T]

  def factory[T](builder: HadoopBuilder[T]): () => Iterator[T] & Closeable =
    () => new ParquetIterator(builder)

  def from(records: RowParquetRecord*): Iterator[RowParquetRecord] & Closeable =
    new Iterator[RowParquetRecord] with Closeable {
      private val wrapped                   = records.iterator
      override def hasNext: Boolean         = wrapped.hasNext
      override def next(): RowParquetRecord = wrapped.next()
      override def close(): Unit            = ()
    }
}

private[parquet4s] class ParquetIterator[T](builder: ParquetIterator.HadoopBuilder[T])
    extends Iterator[T]
    with Closeable {
  private val reader                = builder.build()
  private var recordPreRead         = false
  private var nextRecord: Option[T] = None

  private def preRead(): Unit =
    if (!recordPreRead) {
      nextRecord    = Option(reader.read())
      recordPreRead = true
    }

  override def hasNext: Boolean = {
    preRead()
    nextRecord.nonEmpty
  }

  override def next(): T = {
    preRead()
    nextRecord match {
      case None =>
        throw new NoSuchElementException
      case Some(record) =>
        nextRecord    = None
        recordPreRead = false
        record
    }
  }

  override def close(): Unit = reader.close()
}
