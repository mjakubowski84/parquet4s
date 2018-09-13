package com.mjakubowski84.parquet4s

import java.io.Closeable

import org.apache.hadoop.fs.Path
import org.apache.parquet.hadoop.{ParquetReader => HadoopParquetReader}


object ParquetReader {

  type Builder = HadoopParquetReader.Builder[RowParquetRecord]

  /**
    * Creates new iterable instance of reader.
    * <br/>
    * <b>Note:</b> Remember to call <pre>close()</pre> to clean resources!
    *
    * @param path URI to read the files from
    * @tparam T type of iterable elements
    */
  def apply[T : ParquetRecordDecoder](path: String): ParquetReader[T] =
    apply(HadoopParquetReader.builder[RowParquetRecord](new ParquetReadSupport(), new Path(path)))

  private[mjakubowski84] def apply[T : ParquetRecordDecoder](builder: Builder): ParquetReader[T] =
    new ParquetReaderImpl(builder)
}

trait ParquetReader[T] extends Iterable[T] with Closeable

private class ParquetReaderImpl[T : ParquetRecordDecoder](builder: ParquetReader.Builder) extends ParquetReader[T] {

  private val openCloseables = new scala.collection.mutable.ArrayBuffer[Closeable]()

  override def iterator: Iterator[T] = new Iterator[T] {
    private val reader = builder.build()
    openCloseables.synchronized(openCloseables.append(reader))

    private var recordPreRead = false
    private var nextRecord: Option[T] = None

    override def hasNext: Boolean = {
      if (!recordPreRead) {
        nextRecord = Option(reader.read()).map(ParquetRecordDecoder.decode[T])
        recordPreRead = true
      }
      nextRecord.nonEmpty
    }

    override def next(): T = {
      if (!recordPreRead) {
        nextRecord = Option(reader.read()).map(ParquetRecordDecoder.decode[T])
        recordPreRead = true
      }

      nextRecord match {
        case None =>
          throw new NoSuchElementException
        case Some(record) =>
          nextRecord = None
          recordPreRead = false
          record
      }
    }
  }

  override def close(): Unit = {
    openCloseables.synchronized {
      openCloseables.foreach(_.close())
      openCloseables.clear()
    }
  }

}
