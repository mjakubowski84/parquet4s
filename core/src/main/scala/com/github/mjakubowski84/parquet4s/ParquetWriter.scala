package com.github.mjakubowski84.parquet4s

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

import scala.collection.JavaConverters._


/**
  * Type class that allows to write data which scehma is represented by type <i>T</i> to given path.
  * @tparam T schema of data to write
  */
trait ParquetWriter[T] {

  /**
    * Writes data to given path.
    * @param path location where files are meant to be written
    * @param data data to write
    * @param options configuration of how Parquet files should be created and written
    */
  def write(path: String, data: Iterable[T], options: ParquetWriter.Options)

}


object ParquetWriter  {

  private[parquet4s] type InternalWriter = HadoopParquetWriter[RowParquetRecord]

  private class Builder(path: Path, schema: MessageType) extends HadoopParquetWriter.Builder[RowParquetRecord, Builder](path) {
    private val logger = LoggerFactory.getLogger(ParquetWriter.this.getClass)

    if (logger.isDebugEnabled) {
      logger.debug(s"""Resolved following schema to write Parquet to "$path":\n$schema""")
    }

    override def self(): Builder = this

    override def getWriteSupport(conf: Configuration): WriteSupport[RowParquetRecord] = new ParquetWriteSupport(schema, Map.empty)
  }

  /**
    * Configuration of parquet writer. Please have a look at
    * <a href="https://parquet.apache.org/documentation/latest/">documentation of Parquet</a>
    * to understand what every configuration entry is responsible for.
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
      .build()

  /**
    * Writes iterable collection of data as a Parquet files at given path.
    * Path can represent local file or directory, HDFS, AWS S3, Google Storage, Azure, etc.
    * Please refer to Hadoop client documentation or your data provider in order to know how to configure the connection.
    *
    * @param path URI where the data will be written to
    * @param data Collection of <i>T</> that will be written in Parquet file format
    * @param writer [[ParquetWriter]] that will be used to write data
    * @tparam T type of data, will be used also to resolve the schema of Parquet files
    */
  def write[T](path: String, data: Iterable[T], options: ParquetWriter.Options = ParquetWriter.Options())
              (implicit writer: ParquetWriter[T]): Unit = writer.write(path, data, options)

  /**
    * Default instance of [[ParquetWriter]]
    */
  implicit def writer[T: ParquetRecordEncoder : ParquetSchemaResolver]: ParquetWriter[T] = new ParquetWriter[T] {
    override def write(path: String, data: Iterable[T], options: Options = Options()): Unit = {
      val writer = internalWriter(new Path(path), ParquetSchemaResolver.resolveSchema[T], options)
      val valueCodecConfiguration = options.toValueCodecConfiguration
      try {
        data.foreach { elem =>
          writer.write(ParquetRecordEncoder.encode[T](elem, valueCodecConfiguration))
        }
      } finally {
        writer.close()
      }
    }
  }

}

private class ParquetWriteSupport(schema: MessageType, metadata: Map[String, String]) extends WriteSupport[RowParquetRecord] {
  private var consumer: RecordConsumer = _

  override def init(configuration: Configuration): WriteContext = new WriteContext(schema, metadata.asJava)


  override def write(record: RowParquetRecord): Unit = {
    consumer.startMessage()
    record.fields.foreach {
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
