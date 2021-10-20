package com.github.mjakubowski84.parquet4s

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.parquet.hadoop.{ParquetReader => HadoopParquetReader}
import org.apache.parquet.schema.MessageType

import java.io.Closeable
import java.util.TimeZone
import scala.annotation.implicitNotFound

/** Type class that reads Parquet files from given path.
  * @tparam T
  *   Type that represents schema of Parquet file
  */
@implicitNotFound(
  "Cannot read data of type ${T}. " +
    "Please check if there is implicit ValueCodec available for each field and subfield of ${T}."
)
trait ParquetReader[T] {

  /** Reads data from give path.
    * @param path
    *   URI to location of files
    * @param options
    *   configuration of how Parquet files should be read
    * @param filter
    *   optional before-read filter; no filtering is applied by default; check [[Filter]] for more details
    * @return
    *   iterable collection of data read from path
    */
  def read(
      path: String,
      options: ParquetReader.Options = ParquetReader.Options(),
      filter: Filter                 = Filter.noopFilter
  ): ParquetIterable[T]

}

object ParquetReader {

  type Builder = HadoopParquetReader.Builder[RowParquetRecord]

  /** Configuration settings that are used during decoding or reading Parquet files
    * @param timeZone
    *   set it to [[java.util.TimeZone]] which was used to encode time-based data that you want to read; machine's time
    *   zone is used by default
    * @param hadoopConf
    *   use it to programmatically override Hadoop's [[org.apache.hadoop.conf.Configuration]]
    */
  case class Options(timeZone: TimeZone = TimeZone.getDefault, hadoopConf: Configuration = new Configuration()) {
    private[parquet4s] def toValueCodecConfiguration: ValueCodecConfiguration = ValueCodecConfiguration(timeZone)
  }

  private def newParquetIterable[T: ParquetRecordDecoder](
      path: String,
      options: Options,
      filter: Filter,
      projectedSchemaOpt: Option[MessageType]
  ): ParquetIterable[T] = {
    val valueCodecConfiguration = options.toValueCodecConfiguration
    val hadoopPath              = new Path(path)
    newParquetIterable(
      builder = HadoopParquetReader
        .builder[RowParquetRecord](new ParquetReadSupport(projectedSchemaOpt), hadoopPath)
        .withConf(options.hadoopConf)
        .withFilter(filter.toFilterCompat(valueCodecConfiguration)),
      valueCodecConfiguration = valueCodecConfiguration,
      stats                   = Stats(hadoopPath, options, projectedSchemaOpt, filter)
    )
  }

  private[parquet4s] def newParquetIterable[T: ParquetRecordDecoder](
      builder: Builder,
      valueCodecConfiguration: ValueCodecConfiguration,
      stats: Stats
  ): ParquetIterable[T] =
    new ParquetIterableImpl(builder, valueCodecConfiguration, stats)

  /** Creates new [[ParquetIterable]] over data from given path. <br/> Path can represent local file or directory, HDFS,
    * AWS S3, Google Storage, Azure, etc. Please refer to Hadoop client documentation or your data provider in order to
    * know how to configure the connection.
    *
    * @note
    *   Remember to call `close()` on iterable in order to free resources!
    *
    * @param path
    *   URI to Parquet files, e.g.: {{{"file:///data/users"}}}
    * @param options
    *   configuration of how Parquet files should be read
    * @param filter
    *   optional before-read filtering; no filtering is applied by default; check [[Filter]] for more details
    * @tparam T
    *   type of data that represents the schema of the Parquet file, e.g.:
    *   {{{case class MyData(id: Long, name: String, created: java.sql.Timestamp)}}}
    */
  def read[T](path: String, options: Options = Options(), filter: Filter = Filter.noopFilter)(implicit
      reader: ParquetReader[T]
  ): ParquetIterable[T] =
    reader.read(path, options, filter)

  /** Default implementation of [[ParquetReader]].
    */
  implicit def reader[T: ParquetRecordDecoder]: ParquetReader[T] = new ParquetReader[T] {
    override def read(path: String, options: Options, filter: Filter): ParquetIterable[T] =
      newParquetIterable(path = path, options = options, filter = filter, projectedSchemaOpt = None)
  }

  def withProjection[T: ParquetRecordDecoder: ParquetSchemaResolver]: ParquetReader[T] = new ParquetReader[T] {
    override def read(path: String, options: Options, filter: Filter): ParquetIterable[T] =
      newParquetIterable(
        path               = path,
        options            = options,
        filter             = filter,
        projectedSchemaOpt = Option(ParquetSchemaResolver.resolveSchema[T])
      )
  }

}

/** Allows to iterate over Parquet file(s). Remember to call `close()` when you are done.
  * @tparam T
  *   type that represents schema of Parquet file
  */
trait ParquetIterable[T] extends Iterable[T] with Closeable {

  def min[V: Ordering: ValueCodec](columnPath: String): Option[V]

  def max[V: Ordering: ValueCodec](columnPath: String): Option[V]

}

private class ParquetIterableImpl[T: ParquetRecordDecoder](
    builder: ParquetReader.Builder,
    valueCodecConfiguration: ValueCodecConfiguration,
    stats: Stats
) extends ParquetIterable[T] {

  private val openCloseables = new scala.collection.mutable.ArrayBuffer[Closeable]()

  override def iterator: Iterator[T] = new Iterator[T] {
    private val reader = builder.build()

    openCloseables.synchronized(openCloseables.append(reader))

    private var recordPreRead         = false
    private var nextRecord: Option[T] = None

    override def hasNext: Boolean = {
      if (!recordPreRead) {
        nextRecord = Option(reader.read()).map((ParquetRecordDecoder.decode[T] _).curried(_)(valueCodecConfiguration))
        recordPreRead = true
      }
      nextRecord.nonEmpty
    }

    override def next(): T = {
      if (!recordPreRead) {
        nextRecord = Option(reader.read()).map((ParquetRecordDecoder.decode[T] _).curried(_)(valueCodecConfiguration))
        recordPreRead = true
      }

      nextRecord match {
        case None =>
          throw new NoSuchElementException
        case Some(record) =>
          nextRecord    = None
          recordPreRead = false
          record
      }
    }
  }

  override def close(): Unit =
    openCloseables.synchronized {
      openCloseables.foreach(_.close())
      openCloseables.clear()
    }

  override def min[V: Ordering: ValueCodec](columnPath: String): Option[V] = stats.min(columnPath)

  override def max[V: Ordering: ValueCodec](columnPath: String): Option[V] = stats.max(columnPath)

  override def size: Int = stats.recordCount.toInt

}
