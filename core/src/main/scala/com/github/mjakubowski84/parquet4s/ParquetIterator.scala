package com.github.mjakubowski84.parquet4s

import java.io.Closeable
import org.apache.parquet.hadoop.ParquetFileReader
import org.apache.parquet.ParquetReadOptions
import org.apache.parquet.io.InputFile
import org.apache.parquet.schema.MessageType
import org.apache.parquet.filter2.compat.FilterCompat
import org.apache.hadoop.conf.Configuration
import org.apache.parquet.hadoop.api.InitContext
import scala.jdk.CollectionConverters.*
import java.util.Collections
import org.apache.parquet.io.ColumnIOFactory

private[parquet4s] object ParquetIterator {
  private[parquet4s] type HadoopBuilder[T] = org.apache.parquet.hadoop.ParquetReader.Builder[T]

  def factory[T](builder: HadoopBuilder[T]): () => Iterator[T] & Closeable =
    () => new ParquetIterator(builder)

  def factory(inputFile: InputFile,
  configuration: Configuration,
  projectedSchemaOpt: Option[MessageType],
  columnProjections: Seq[ColumnProjection],
  filter: FilterCompat.Filter,
  metadataReader: MetadataReader): () => Iterator[RowParquetRecord] & Closeable =
    () => new InternalParquetIterator(inputFile, configuration, projectedSchemaOpt, columnProjections, filter, metadataReader)


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

private[parquet4s] class InternalParquetIterator(
  inputFile: InputFile,
  configuration: Configuration,
  projectedSchemaOpt: Option[MessageType],
  columnProjections: Seq[ColumnProjection],
  filter: FilterCompat.Filter,
  metadataReader: MetadataReader
) extends Iterator[RowParquetRecord]
    with Closeable {
    
  val options = ParquetReadOptions.builder().withRecordFilter(filter).build()
  options.getPropertyNames().forEach(property => configuration.set(property, options.getProperty(property)))
  // TODO building options should happen only once, not per each file    

  val reader = ParquetFileReader.open(inputFile, options)
  projectedSchemaOpt.foreach(reader.setRequestedSchema)
  val fileMetadata = reader.getFooter().getFileMetaData();
  val fileSchema = fileMetadata.getSchema()

  val readSupport = new ParquetReadSupport(projectedSchemaOpt, columnProjections, metadataReader)
  val readContext = readSupport.init(
    new InitContext(
      configuration, 
      fileMetadata.getKeyValueMetaData().asScala.view.mapValues(Collections.singleton[String]).toMap.asJava, 
      fileSchema
    )
  )
  val columnIOFactory = new ColumnIOFactory(fileMetadata.getCreatedBy())
  val columnIO = columnIOFactory.getColumnIO(projectedSchemaOpt.getOrElse(fileSchema), fileSchema, /* strictTypeChecking */ true)
  val recordConverter = readSupport.prepareForRead(configuration, fileMetadata.getKeyValueMetaData(), fileSchema, readContext)

  val wrappedIterator = Iterator
    .continually(reader.readNextFilteredRowGroup())
    .takeWhile(_ != null)
    .flatMap { pageStore =>
      val recordReader = columnIO.getRecordReader(pageStore, recordConverter, filter)
      Iterator.range(0, pageStore.getRowCount())
        .map(_ => Option(recordReader.read()))
        .collect { case Some(record) if !recordReader.shouldSkipCurrentRecord() => record}
    }


  override def hasNext: Boolean = wrappedIterator.hasNext

  override def next(): RowParquetRecord = wrappedIterator.next()

  override def close(): Unit = reader.close()



}