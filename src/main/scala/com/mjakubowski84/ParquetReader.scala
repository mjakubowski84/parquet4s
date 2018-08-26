package com.mjakubowski84

import java.io.Closeable

import org.apache.hadoop.fs.Path
import org.apache.parquet.hadoop.{ParquetReader => HadoopParquetReader}


object ParquetReader {

  type Builder = HadoopParquetReader.Builder[ParquetRecord]

  /**
    * Creates new iterable instance of reader.
    * <br/>
    * <b>Note:</b> Remember to call <pre>close()</pre> to clean resources!
    *
    * @param path URI to read the files from
    * @tparam T type of iterable elements
    */
  def apply[T : MapReader](path: String): ParquetReader[T] =
    apply(HadoopParquetReader.builder[ParquetRecord](new ParquetReadSupport(), new Path(path)))

  private[mjakubowski84] def apply[T : MapReader](builder: Builder): ParquetReader[T] =
    new ParquetReaderImpl(builder)
}

trait ParquetReader[T] extends Iterable[T] with Closeable

private class ParquetReaderImpl[T : MapReader](builder: ParquetReader.Builder) extends ParquetReader[T] {

  private val openCloseables = new scala.collection.mutable.ArrayBuffer[Closeable]()

  override def iterator: Iterator[T] = new Iterator[T] {
    private val reader = builder.build()
    openCloseables.synchronized(openCloseables.append(reader))

    private var nextPreCalled = false
    private var nextValue: Option[T] = None

    override def hasNext: Boolean = {
      if (!nextPreCalled) {
        nextPreCalled = true
        nextValue = Option(reader.read()).map(_.toObject)
      }
      nextValue.nonEmpty
    }

    override def next(): T = {
      if (!nextPreCalled) {
        nextValue = Option(reader.read()).map(_.toObject)
      }
      nextPreCalled = false

      nextValue match {
        case None =>
          Option(reader.read()).getOrElse(throw new NoSuchElementException).toObject
        case Some(element) =>
          nextValue = None
          element
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
