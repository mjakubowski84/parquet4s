package com.github.mjakubowski84.parquet4s

import java.io.Closeable

import org.apache.hadoop.fs.Path
import org.apache.parquet.hadoop.{ParquetReader => HadoopParquetReader}

/**
  * Holds factory that builds iterable instance of Parquet data source.
  */
object ParquetReader {

  type Builder = HadoopParquetReader.Builder[RowParquetRecord]

  /**
    * Creates new iterable instance of reader.
    * <br/>
    * Path can refer to local file, HDFS, AWS S3, Google Storage, Azure, etc.
    * Please refer to Hadoop client documentation or your data provider in order to know how to configure the connection.
    * <br/>
    * <b>Note:</b> Remember to call {{{ close() }}} to clean resources!
    *
    * @param path URI to Parquet files, e.g.:
    *             {{{ "file:///data/users" }}}
    * @tparam T type of data that represent the schema of the Parquet data, e.g.:
    *           {{{
    *              case class MyData(id: Long, name: String, created: java.sql.Timestamp)
    *           }}}
    */
  def apply[T : ParquetRecordDecoder](path: String): ParquetReader[T] =
    apply(HadoopParquetReader.builder[RowParquetRecord](new ParquetReadSupport(), new Path(path)))

  private[mjakubowski84] def apply[T : ParquetRecordDecoder](builder: Builder): ParquetReader[T] =
    new ParquetReaderImpl(builder)

  // TODO introduce type class for reader with a single implicit implementation so that there won't be need for users to write import ParquetRecordEncoder._
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
