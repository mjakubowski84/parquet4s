package com.github.mjakubowski84.parquet4s

import java.io.Closeable

import org.apache.hadoop.fs.Path
import org.apache.parquet.hadoop.{ParquetReader => HadoopParquetReader}

/**
  * Type class that reads Parquet files from given path.
  * @tparam T Type that represents schema of Parquet file
  */
trait ParquetReader[T] {

  /**
    * Reads data from give path.
    * @param path URI to location of files
    * @return iterable collection of data read from path
    */
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
    * Creates new [[ParquetIterable]] over data from given path.
    * <br/>
    * Path can represent local file or directory, HDFS, AWS S3, Google Storage, Azure, etc.
    * Please refer to Hadoop client documentation or your data provider in order to know how to configure the connection.
    * <br/>
    * <b>Note:</b> Remember to call {{{ close() }}} on iterable in order to free resources!
    *
    * @param path URI to Parquet files, e.g.:
    *             {{{ "file:///data/users" }}}
    * @tparam T type of data that represents the schema of the Parquet file, e.g.:
    *           {{{ case class MyData(id: Long, name: String, created: java.sql.Timestamp) }}}
    */
  def read[T](path: String)(implicit reader: ParquetReader[T]): ParquetIterable[T] = reader.read(path)

  /**
    * Default implementation of [[ParquetReader]].
    */
  implicit def reader[T : ParquetRecordDecoder]: ParquetReader[T] = new ParquetReader[T] {
    override def read(path: String): ParquetIterable[T] = newParquetIterable(path)
  }

}

/**
  * Allows to iterate over Parquet file(s). Remember to call {{{close()}}} when you are done.
  * @tparam T type that represents schema of Parquet file
  */
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
