package com.github.mjakubowski84.parquet4s

import java.io.Closeable

import org.apache.hadoop.fs.Path
import org.apache.parquet.hadoop.{ParquetReader => HadoopParquetReader}


trait ParquetReader[T] {

  def read(path: String): ParquetIterable[T]

}

object ParquetReader {

  type Builder = HadoopParquetReader.Builder[RowParquetRecord]

  @deprecated(message = "Please use read function or ParquetReader type class", since = "0.3.0")
  def apply[T : ParquetRecordDecoder](path: String): ParquetIterable[T] = newParquetIterable(path)

  private def newParquetIterable[T : ParquetRecordDecoder](path: String): ParquetIterable[T] =
    newParquetIterable(HadoopParquetReader.builder[RowParquetRecord](new ParquetReadSupport(), new Path(path)))

  private[parquet4s] def newParquetIterable[T : ParquetRecordDecoder](builder: Builder): ParquetIterable[T] =
    new ParquetIterableImpl(builder)

  /**
    * Creates new object that iterates over Parquet data.
    * <br/>
    * Path can refer to local file, HDFS, AWS S3, Google Storage, Azure, etc.
    * Please refer to Hadoop client documentation or your data provider in order to know how to configure the connection.
    * <br/>
    * <b>Note:</b> Remember to call {{{ close() }}} on iterable in order to free resources!
    *
    * @param path URI to Parquet files, e.g.:
    *             {{{ "file:///data/users" }}}
    * @tparam T type of data that represent the schema of the Parquet data, e.g.:
    *           {{{
    *              case class MyData(id: Long, name: String, created: java.sql.Timestamp)
    *           }}}
    */
  def read[T](path: String)(implicit reader: ParquetReader[T]): ParquetIterable[T] = reader.read(path)

  implicit def reader[T : ParquetRecordDecoder]: ParquetReader[T] = new ParquetReader[T] {
    override def read(path: String): ParquetIterable[T] = newParquetIterable(path)
  }

}

trait ParquetIterable[T] extends Iterable[T] with Closeable

private class ParquetIterableImpl[T : ParquetRecordDecoder](builder: ParquetReader.Builder) extends ParquetIterable[T] {

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
