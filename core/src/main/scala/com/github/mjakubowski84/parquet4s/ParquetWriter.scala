package com.github.mjakubowski84.parquet4s

import java.io.Closeable
import java.util.TimeZone
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.parquet.hadoop.api.WriteSupport
import org.apache.parquet.hadoop.api.WriteSupport.WriteContext
import org.apache.parquet.hadoop.metadata.CompressionCodecName
import org.apache.parquet.hadoop.{ParquetFileWriter, ParquetWriter => HadoopParquetWriter}
import org.apache.parquet.io.api.RecordConsumer
import org.apache.parquet.schema.MessageType
import org.slf4j.LoggerFactory

import scala.annotation.implicitNotFound
import scala.jdk.CollectionConverters._


/**
  * Type class that allows to write data which schema is represented by type <i>T</i>.
  * Path and options are meant to be set by implementation of the trait.
  * @tparam T schema of data to write
  */
trait ParquetWriter[T] extends Closeable {

  /**
    * Appends data chunk to file contents.
    * @param data data to write
    */
  def write(data: Iterable[T]): Unit

  /**
    * Appends data chunk to file contents.
    * @param data data to write
    */
  def write(data: T*): Unit

}


object ParquetWriter  {

  private[parquet4s] type InternalWriter = HadoopParquetWriter[RowParquetRecord]

  @implicitNotFound("Cannot write data of type ${T}. " +
      "Please check if there are implicit ValueCodec and TypedSchemaDef available for each field and subfield of ${T}."
  )
  type ParquetWriterFactory[T] = (String, Options) => ParquetWriter[T]

  private val SignatureMetadata = Map("MadeBy" -> "https://github.com/mjakubowski84/parquet4s")

  private class Builder(path: Path, schema: MessageType) extends HadoopParquetWriter.Builder[RowParquetRecord, Builder](path) {
    private val logger = LoggerFactory.getLogger(ParquetWriter.this.getClass)

    if (logger.isDebugEnabled) {
      logger.debug(s"""Resolved following schema to write Parquet to "$path":\n$schema""")
    }

    override def self(): Builder = this

    override def getWriteSupport(conf: Configuration): WriteSupport[RowParquetRecord] =
      new ParquetWriteSupport(schema, SignatureMetadata)
  }

  /**
    * Configuration of parquet writer. Please have a look at
    * <a href="https://parquet.apache.org/documentation/latest/">documentation of Parquet</a>
    * to understand what every configuration entry is responsible for.
    * Apart from options specific for Parquet file format there are some other - what follows:
    * @param hadoopConf can be used to programmatically set Hadoop's [[org.apache.hadoop.conf.Configuration]]
    * @param timeZone used when encoding time-based data, local machine's time zone is used by default
    */
  case class Options(
                    writeMode: ParquetFileWriter.Mode = ParquetFileWriter.Mode.CREATE,
                    compressionCodecName: CompressionCodecName = HadoopParquetWriter.DEFAULT_COMPRESSION_CODEC_NAME,
                    dictionaryEncodingEnabled: Boolean = HadoopParquetWriter.DEFAULT_IS_DICTIONARY_ENABLED,
                    dictionaryPageSize: Int = HadoopParquetWriter.DEFAULT_PAGE_SIZE,
                    maxPaddingSize: Int = HadoopParquetWriter.MAX_PADDING_SIZE_DEFAULT,
                    pageSize: Int = HadoopParquetWriter.DEFAULT_PAGE_SIZE,
                    rowGroupSize: Int = HadoopParquetWriter.DEFAULT_BLOCK_SIZE,
                    validationEnabled: Boolean = HadoopParquetWriter.DEFAULT_IS_VALIDATING_ENABLED,
                    hadoopConf: Configuration = new Configuration(),
                    timeZone: TimeZone = TimeZone.getDefault
                    ) {
    private[parquet4s] def toValueCodecConfiguration: ValueCodecConfiguration = ValueCodecConfiguration(timeZone)
  }

  private[parquet4s] def internalWriter(path: Path, schema: MessageType, options: Options): InternalWriter =
    new Builder(path, schema)
      .withWriteMode(options.writeMode)
      .withCompressionCodec(options.compressionCodecName)
      .withDictionaryEncoding(options.dictionaryEncodingEnabled)
      .withDictionaryPageSize(options.dictionaryPageSize)
      .withMaxPaddingSize(options.maxPaddingSize)
      .withPageSize(options.pageSize)
      .withRowGroupSize(options.rowGroupSize)
      .withValidation(options.validationEnabled)
      .withConf(options.hadoopConf)
      .build()

  /**
    * Writes iterable collection of data as a Parquet files at given path.
    * Path can represent local file or directory, HDFS, AWS S3, Google Storage, Azure, etc.
    * Please refer to Hadoop client documentation or your data provider in order to know how to configure the connection.
    *
    * @param path URI where the data will be written to
    * @param data Collection of <i>T</> that will be written in Parquet file format
    * @param options configuration of writer, see [[ParquetWriter.Options]]
    * @param writerFactory [[ParquetWriterFactory]] that will be used to create an instance of writer
    * @tparam T type of data, will be used also to resolve the schema of Parquet files
    */
  def writeAndClose[T](path: String, data: Iterable[T], options: ParquetWriter.Options = ParquetWriter.Options())
                      (implicit writerFactory: ParquetWriterFactory[T]): Unit = {
    val writer = writerFactory(path, options)
    try writer.write(data)
    finally writer.close()
  }

  def writer[T](path: String, options: ParquetWriter.Options = ParquetWriter.Options())
               (implicit writerFactory: ParquetWriterFactory[T]): ParquetWriter[T] =
    writerFactory(path, options)


  /**
    * Default instance of [[ParquetWriterFactory]]
    */
  implicit def writerFactory[T: ParquetRecordEncoder : ParquetSchemaResolver]: ParquetWriterFactory[T] = (path, options) =>
    new DefaultParquetWriter[T](path, options)

}

private class DefaultParquetWriter[T : ParquetRecordEncoder : ParquetSchemaResolver](
                                                                                      path: String,
                                                                                      options: ParquetWriter.Options
                                                                                    ) extends ParquetWriter[T] {
  private val internalWriter = ParquetWriter.internalWriter(
    path = new Path(path),
    schema = ParquetSchemaResolver.resolveSchema[T],
    options = options
  )
  private val valueCodecConfiguration = options.toValueCodecConfiguration
  private val logger = LoggerFactory.getLogger(this.getClass)
  private var closed = false

  override def write(data: Iterable[T]): Unit = {
    if (closed) {
      throw new IllegalStateException("Attempted to write with a writer which was already closed")
    } else {
      data.foreach { elem =>
        internalWriter.write(ParquetRecordEncoder.encode[T](elem, valueCodecConfiguration))
      }
    }
  }

  override def write(data: T*): Unit = this.write(data)

  override def close(): Unit = synchronized {
    if (closed) {
      logger.warn("Attempted to close a writer which was already closed")
    } else {
      if (logger.isDebugEnabled) {
        logger.debug(s"Finished writing to $path and closing writer.")
      }
      closed = true
      internalWriter.close()
    }
  }

}

private class ParquetWriteSupport(schema: MessageType, metadata: Map[String, String]) extends WriteSupport[RowParquetRecord] {
  private var consumer: RecordConsumer = _

  override def init(configuration: Configuration): WriteContext = new WriteContext(schema, metadata.asJava)

  override def write(record: RowParquetRecord): Unit = {
    consumer.startMessage()
    record.iterator.foreach {
      case (_, NullValue) =>
        // ignoring nulls
      case (name, value) =>
        val fieldIndex = schema.getFieldIndex(name)
        consumer.startField(name, fieldIndex)
        value.write(schema.getType(fieldIndex), consumer)
        consumer.endField(name, fieldIndex)
    }
    consumer.endMessage()
  }

  override def prepareForWrite(recordConsumer: RecordConsumer): Unit = {
    consumer = recordConsumer
  }
}
