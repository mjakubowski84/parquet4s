package com.github.mjakubowski84.parquet4s

import java.io.Closeable
import org.apache.parquet.hadoop.ParquetFileReader
import org.apache.parquet.ParquetReadOptions
import org.apache.parquet.io.InputFile
import org.apache.parquet.schema.MessageType
import org.apache.parquet.filter2.compat.FilterCompat
import org.apache.parquet.hadoop.api.InitContext
import java.util.Collections
import org.apache.parquet.io.ColumnIOFactory
import org.apache.parquet.column.page.PageReadStore
import java.util.HashMap as JHashMap
import java.util.Map as JMap
import java.util.Map.Entry as JEntry
import java.util.Set as JSet
import scala.annotation.tailrec
import compat.IteratorCompat

private[parquet4s] object ParquetIterator {
  private[parquet4s] type HadoopBuilder[T] = org.apache.parquet.hadoop.ParquetReader.Builder[T]

  def factory[T](builder: HadoopBuilder[T]): () => Iterator[T] & Closeable =
    () => new ParquetIterator(builder)

  def factory(
      inputFile: InputFile,
      projectedSchemaOpt: Option[MessageType],
      columnProjections: Seq[ColumnProjection],
      filter: FilterCompat.Filter,
      metadataReader: MetadataReader,
      readerOptions: ParquetReader.Options
  ): () => Iterator[RowParquetRecord] & Closeable =
    () =>
      new InternalParquetIterator(
        inputFile,
        projectedSchemaOpt,
        columnProjections,
        filter,
        metadataReader,
        readerOptions
      )

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
    projectedSchemaOpt: Option[MessageType],
    columnProjections: Seq[ColumnProjection],
    filter: FilterCompat.Filter,
    metadataReader: MetadataReader,
    readerOptions: ParquetReader.Options
) extends Iterator[RowParquetRecord]
    with Closeable {

  val configuration = readerOptions.hadoopConf
  val options = ParquetReadOptions
    .builder()
    .withRecordFilter(filter)
    .withUseHadoopVectoredIo(readerOptions.useHadoopVectoredIo)
    .build()
  options.getPropertyNames().forEach(property => configuration.set(property, options.getProperty(property)))
  // TODO building options should happen only once, not per each file

  val reader = ParquetFileReader.open(inputFile, options)

  projectedSchemaOpt.foreach(reader.setRequestedSchema)
  val fileMetadata = reader.getFooter().getFileMetaData();
  val fileSchema   = fileMetadata.getSchema()

  val readSupport = new ParquetReadSupport(projectedSchemaOpt, columnProjections, metadataReader)
  val readContext = readSupport.init(
    new InitContext(
      configuration,
      toSetMultiMap(fileMetadata.getKeyValueMetaData()),
      fileSchema
    )
  )
  val columnIOFactory = new ColumnIOFactory(fileMetadata.getCreatedBy())
  val columnIO =
    columnIOFactory.getColumnIO(projectedSchemaOpt.getOrElse(fileSchema), fileSchema, /* strictTypeChecking */ true)
  val recordConverter =
    readSupport.prepareForRead(configuration, fileMetadata.getKeyValueMetaData(), fileSchema, readContext)

  private class Paginator(pageStore: PageReadStore) {
    lazy val recordReader  = columnIO.getRecordReader(pageStore, recordConverter, filter)
    lazy val size          = pageStore.getRowCount()
    var currentRecordIndex = 0L

    @tailrec
    final def next(): Option[(RowParquetRecord, Paginator)] =
      if (pageStore == null) {
        None
      } else if (currentRecordIndex < size) {
        val record = recordReader.read()
        currentRecordIndex = currentRecordIndex + 1L
        // shouldSkipCurrentRecord is actually yet anothet null check, but let's keep it in case the implementation changes in the future
        if (record == null || recordReader.shouldSkipCurrentRecord()) {
          next()
        } else {
          Some(record -> this)
        }
      } else {
        new Paginator(reader.readNextFilteredRowGroup()).next()
      }

  }

  lazy val wrappedIterator =
    IteratorCompat.unfold[RowParquetRecord, Paginator](new Paginator(reader.readNextFilteredRowGroup()))(_.next())

  override def hasNext: Boolean = wrappedIterator.hasNext

  override def next(): RowParquetRecord = wrappedIterator.next()

  override def close(): Unit = reader.close()

  private def toSetMultiMap(in: JMap[String, String]): JMap[String, JSet[String]] =
    in.entrySet()
      .stream()
      .collect[JHashMap[String, JSet[String]]](
        () => new JHashMap[String, JSet[String]](),
        (m: JMap[String, JSet[String]], entry: JEntry[String, String]) =>
          m.put(entry.getKey(), Collections.singleton(entry.getValue())),
        (m1: JMap[String, JSet[String]], m2: JMap[String, JSet[String]]) => m1.putAll(m2)
      )

  // private def getUnsafe: Unsafe = {
  //   val f = classOf[Unsafe].getDeclaredField("theUnsafe");
  //   f.setAccessible(true);
  //   f.get(null).asInstanceOf[Unsafe];
  // }

}
