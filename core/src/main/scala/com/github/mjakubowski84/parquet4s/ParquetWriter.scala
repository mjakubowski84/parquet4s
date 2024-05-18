package com.github.mjakubowski84.parquet4s

import org.apache.hadoop.conf.Configuration
import org.apache.parquet.hadoop.api.WriteSupport
import org.apache.parquet.hadoop.api.WriteSupport.{FinalizedWriteContext, WriteContext}
import org.apache.parquet.hadoop.metadata.CompressionCodecName
import org.apache.parquet.hadoop.{ParquetFileWriter, ParquetWriter as HadoopParquetWriter}
import org.apache.parquet.io.OutputFile
import org.apache.parquet.io.api.RecordConsumer
import org.apache.parquet.schema.MessageType
import org.slf4j.LoggerFactory

import java.io.Closeable
import java.util.TimeZone
import scala.annotation.implicitNotFound
import scala.jdk.CollectionConverters.*
import scala.util.Using
import scala.util.control.NonFatal

/** Type class that allows to write data which schema is represented by type <i>T</i>. Path and options are meant to be
  * set by implementation of the trait.
  * @tparam T
  *   schema of data to write
  */
trait ParquetWriter[T] extends Closeable {

  /** Appends data chunk to file contents.
    * @param data
    *   data to write
    */
  def write(data: Iterable[T]): Unit

  /** Appends data chunk to file contents.
    * @param data
    *   data to write
    */
  def write(data: T*): Unit

}

object ParquetWriter {

  private[parquet4s] type InternalWriter = HadoopParquetWriter[RowParquetRecord]

  @implicitNotFound(
    "Cannot write data of type ${T}. " +
      "Please check if there are implicit ValueEncoder and TypedSchemaDef available for each field and subfield of ${T}."
  )
  @deprecated("2.0.0", "Use builder api by calling 'of[T]' or 'generic'")
  type ParquetWriterFactory[T] = (Path, Options) => ParquetWriter[T]

  private val SignatureMetadata = Map("MadeBy" -> "https://github.com/mjakubowski84/parquet4s")

  private class InternalBuilder(file: OutputFile, schema: MessageType, extraMetadata: MetadataWriter)
      extends HadoopParquetWriter.Builder[RowParquetRecord, InternalBuilder](file) {
    private val logger = LoggerFactory.getLogger(ParquetWriter.this.getClass)

    if (logger.isDebugEnabled) {
      logger.debug(s"""Resolved following schema to write Parquet to "$file":\n$schema""")
    }

    override def self(): InternalBuilder = this

    override def getWriteSupport(conf: Configuration): WriteSupport[RowParquetRecord] =
      new ParquetWriteSupport(schema, SignatureMetadata, extraMetadata)
  }

  /** Configuration of parquet writer. Please have a look at <a href="https://parquet.apache.org/docs/">documentation of
    * Parquet</a> to understand what every configuration entry is responsible for. <br> <b>NOTE!</b> Please be careful
    * when using OVERWRITE mode. All data at given path (either file or directory) are deleted before writing in the
    * OVERWRITE mode. <br> Apart from options specific for Parquet file format there are some other:
    * @param hadoopConf
    *   can be used to programmatically set Hadoop's [[org.apache.hadoop.conf.Configuration]]
    * @param timeZone
    *   used when encoding time-based data, local machine's time zone is used by default
    */
  case class Options(
      writeMode: ParquetFileWriter.Mode          = ParquetFileWriter.Mode.CREATE,
      compressionCodecName: CompressionCodecName = HadoopParquetWriter.DEFAULT_COMPRESSION_CODEC_NAME,
      dictionaryEncodingEnabled: Boolean         = HadoopParquetWriter.DEFAULT_IS_DICTIONARY_ENABLED,
      dictionaryPageSize: Int                    = HadoopParquetWriter.DEFAULT_PAGE_SIZE,
      maxPaddingSize: Int                        = HadoopParquetWriter.MAX_PADDING_SIZE_DEFAULT,
      pageSize: Int                              = HadoopParquetWriter.DEFAULT_PAGE_SIZE,
      rowGroupSize: Long                         = HadoopParquetWriter.DEFAULT_BLOCK_SIZE,
      validationEnabled: Boolean                 = HadoopParquetWriter.DEFAULT_IS_VALIDATING_ENABLED,
      hadoopConf: Configuration                  = new Configuration(),
      timeZone: TimeZone                         = TimeZone.getDefault
  ) {
    private[parquet4s] def applyTo[T, B <: HadoopParquetWriter.Builder[T, B]](builder: B): B =
      builder
        .withWriteMode(writeMode)
        .withCompressionCodec(compressionCodecName)
        .withDictionaryEncoding(dictionaryEncodingEnabled)
        .withDictionaryPageSize(dictionaryPageSize)
        .withMaxPaddingSize(maxPaddingSize)
        .withPageSize(pageSize)
        .withRowGroupSize(rowGroupSize)
        .withValidation(validationEnabled)
        .withConf(hadoopConf)
  }

  /** Builder of [[ParquetWriter]].
    * @tparam T
    *   type of documents to write
    */
  trait Builder[T] {

    /** Configuration of writer, see [[ParquetWriter.Options]]
      */
    def options(options: Options): Builder[T]

    /** Builds a writer for writing the output file
      */
    @experimental
    def build(file: OutputFile): ParquetWriter[T]

    def build(path: Path): ParquetWriter[T]

    /** Writes iterable collection of data as a Parquet output file.
      */
    @experimental
    def writeAndClose(file: OutputFile, data: Iterable[T]): Unit

    /** Writes iterable collection of data as a Parquet files at given path.
      */
    def writeAndClose(path: Path, data: Iterable[T]): Unit

  }

  trait CustomBuilder[T] {

    /** @param options
      *   writer options
      */
    def options(options: ParquetWriter.Options): CustomBuilder[T]

    def writeAndClose(data: Iterable[T]): Unit

    def build(): ParquetWriter[T]

  }

  private case class CustomBuilderImpl[T, B <: HadoopParquetWriter.Builder[T, B]](
      builder: B,
      options: Options = Options()
  ) extends CustomBuilder[T] {
    override def options(options: Options): CustomBuilder[T] = this.copy(options = options)

    override def writeAndClose(data: Iterable[T]): Unit = Using.resource(build())(_.write(data))

    override def build(): ParquetWriter[T] =
      new ParquetWriterImpl[T, T](
        internalWriter  = options.applyTo[T, B](builder).build(),
        encode          = identity,
        fileDescription = "custom"
      )

  }

  private case class BuilderImpl[T](options: Options = Options())(implicit
      encoder: ParquetRecordEncoder[T],
      schemaResolver: ParquetSchemaResolver[T]
  ) extends Builder[T] {
    override def options(options: Options): Builder[T] = this.copy(options = options)
    override def build(file: OutputFile): ParquetWriter[T] = new ParquetWriterImpl[T, RowParquetRecord](
      internalWriter = internalWriter(
        file           = file,
        schema         = ParquetSchemaResolver.resolveSchema[T],
        metadataWriter = encoder,
        options        = options
      ),
      encode          = ParquetRecordEncoder.encode(_, ValueCodecConfiguration(options)),
      fileDescription = file.toString
    )
    override def build(path: Path): ParquetWriter[T] = build(path.toOutputFile(options))

    override def writeAndClose(file: OutputFile, data: Iterable[T]): Unit =
      Using.resource(build(file))(_.write(data))

    override def writeAndClose(path: Path, data: Iterable[T]): Unit =
      Using.resource(build(path))(_.write(data))

  }

  private[parquet4s] def internalWriter(
      file: OutputFile,
      schema: MessageType,
      metadataWriter: MetadataWriter,
      options: Options
  ): InternalWriter =
    options
      .applyTo[RowParquetRecord, InternalBuilder](new InternalBuilder(file, schema, metadataWriter))
      .build()

  /** Writes iterable collection of data as a Parquet files at given path. Path can represent local file or directory,
    * HDFS, AWS S3, Google Storage, Azure, etc. Please refer to Hadoop client documentation or your data provider in
    * order to know how to configure the connection.
    *
    * @param path
    *   [[Path]] where the data will be written to
    * @param data
    *   Collection of <i>T</> that will be written in Parquet file format
    * @param options
    *   configuration of writer, see [[ParquetWriter.Options]]
    * @param writerFactory
    *   [[ParquetWriterFactory]] that will be used to create an instance of writer
    * @tparam T
    *   type of data, will be used also to resolve the schema of Parquet files
    */
  @deprecated("2.0.0", "Use builder api by calling 'of[T]' or 'generic'")
  def writeAndClose[T](path: Path, data: Iterable[T], options: ParquetWriter.Options = ParquetWriter.Options())(implicit
      writerFactory: ParquetWriterFactory[T]
  ): Unit =
    Using.resource(writerFactory(path, options))(_.write(data))

  @deprecated("2.0.0", "Use builder api by calling 'of[T]' or 'generic''")
  def writer[T](path: Path, options: ParquetWriter.Options = ParquetWriter.Options())(implicit
      writerFactory: ParquetWriterFactory[T]
  ): ParquetWriter[T] =
    writerFactory(path, options)

  /** Default instance of [[ParquetWriterFactory]]
    */
  @deprecated("2.0.0", "Use builder api by calling 'of[T]' or 'generic'")
  implicit def writerFactory[T: ParquetRecordEncoder: ParquetSchemaResolver]: ParquetWriterFactory[T] =
    (path, options) => {
      val vcc = ValueCodecConfiguration(options)
      new ParquetWriterImpl[T, RowParquetRecord](
        internalWriter = internalWriter(
          file           = path.toOutputFile(options),
          schema         = ParquetSchemaResolver.resolveSchema[T],
          metadataWriter = implicitly[ParquetRecordEncoder[T]],
          options        = options
        ),
        encode          = entity => ParquetRecordEncoder.encode(entity, vcc),
        fileDescription = path.toString
      )
    }

  /** Creates [[Builder]] of [[ParquetWriter]] for documents of type <i>T</i>.
    */
  def of[T: ParquetRecordEncoder: ParquetSchemaResolver]: Builder[T] = BuilderImpl()

  /** Creates [[Builder]] of [[ParquetWriter]] for generic records.
    */
  def generic(message: MessageType): Builder[RowParquetRecord] =
    BuilderImpl()(RowParquetRecord.genericParquetRecordEncoder, RowParquetRecord.genericParquetSchemaResolver(message))

  @experimental
  def custom[T, B <: HadoopParquetWriter.Builder[T, B]](builder: B): CustomBuilder[T] = CustomBuilderImpl(builder)

}

private class ParquetWriterImpl[T, W](
    internalWriter: HadoopParquetWriter[W],
    encode: T => W,
    fileDescription: String
) extends ParquetWriter[T] {
  private val logger = LoggerFactory.getLogger(this.getClass)
  private var closed = false

  override def write(data: Iterable[T]): Unit =
    if (closed) {
      throw new IllegalStateException("Attempted to write with a writer which was already closed")
    } else {
      data.foreach { elem =>
        internalWriter.write(encode(elem))
      }
    }

  override def write(data: T*): Unit = this.write(data)

  override def close(): Unit = synchronized {
    if (closed) {
      logger.warn("Attempted to close a writer which was already closed")
    } else {
      if (logger.isDebugEnabled) {
        logger.debug(s"Finished writing to $fileDescription and closing writer.")
      }
      closed = true
      try internalWriter.close()
      catch {
        case NonFatal(_) => // ignores bug in Parquet
      }
    }
  }

}

private class ParquetWriteSupport(schema: MessageType, metadata: Map[String, String], extraMetadata: MetadataWriter)
    extends WriteSupport[RowParquetRecord] {
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

  override def prepareForWrite(recordConsumer: RecordConsumer): Unit =
    consumer = recordConsumer

  override def finalizeWrite(): FinalizedWriteContext =
    new FinalizedWriteContext(extraMetadata.getMetadata().asJava)
}
