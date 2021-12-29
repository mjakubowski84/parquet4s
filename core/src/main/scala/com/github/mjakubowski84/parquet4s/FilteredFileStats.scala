package com.github.mjakubowski84.parquet4s

import org.apache.hadoop.fs.FileStatus
import org.apache.parquet.ParquetReadOptions
import org.apache.parquet.hadoop.ParquetFileReader
import org.apache.parquet.hadoop.util.HadoopInputFile
import org.apache.parquet.io.api.*
import org.apache.parquet.io.{ColumnIOFactory, MessageColumnIO}
import org.apache.parquet.schema.{GroupType, MessageType}

import scala.annotation.tailrec
import scala.jdk.CollectionConverters.*

/** Calculates statistics from <b>filtered</b> Parquet files.
  */
private[parquet4s] class FilteredFileStats(
    status: FileStatus,
    options: ParquetReader.Options,
    projectionSchemaOpt: Option[MessageType],
    filter: Filter
) extends Stats {

  private val vcc           = ValueCodecConfiguration(options)
  private val inputFile     = HadoopInputFile.fromStatus(status, options.hadoopConf)
  private val readerOptions = ParquetReadOptions.builder().withRecordFilter(filter.toFilterCompat(vcc)).build()

  abstract private class StatsReader {
    protected val reader: ParquetFileReader = ParquetFileReader.open(inputFile, readerOptions)
    projectionSchemaOpt.foreach(reader.setRequestedSchema)

    private val fileMetaData                   = reader.getFooter.getFileMetaData
    private val messageSchema                  = fileMetaData.getSchema
    protected val requestedSchema: MessageType = projectionSchemaOpt.getOrElse(messageSchema)
    private val columnIOFactory                = new ColumnIOFactory(fileMetaData.getCreatedBy)
    protected val columnIO: MessageColumnIO    = columnIOFactory.getColumnIO(requestedSchema, messageSchema, true)

    def close(): Unit = reader.close()
  }

  private class RecordCountReader extends StatsReader {
    def filteredRecordCount: Long =
      Iterator
        .continually(reader.readNextFilteredRowGroup())
        .takeWhile(_ != null)
        .flatMap { store =>
          val recordReader = columnIO.getRecordReader(
            store,
            new RowCountMaterializer(requestedSchema),
            filter.toFilterCompat(vcc)
          )
          (0L until store.getRowCount.longValue()).iterator
            .map(_ => Option(recordReader.read()))
            .collect { case Some(v) => v.value }
        }
        .sum
  }

  private class MinMaxReader[V](columnPath: ColumnPath, startExtremeOpt: Option[V])(implicit
      decoder: ValueDecoder[V],
      ordering: Ordering[V]
  ) extends StatsReader {
    private val dotString = columnPath.toString
    private lazy val currentBlockField = {
      val f = reader.getClass.getDeclaredField("currentBlock")
      f.setAccessible(true)
      f
    }
    private lazy val numOfBlocks = reader.getRowGroups.size()
    private def currentBlock     = currentBlockField.getInt(reader).intValue()
    private def currentRowGroupStatistics =
      reader.getRowGroups
        .get(currentBlock)
        .getColumns
        .asScala
        .find(_.getPath.toDotString == dotString)
        .map(_.getStatistics)

    private def currentRowGroupStatisticsMin =
      currentRowGroupStatistics.flatMap(statsMinValue).map(value => decoder.decode(value, vcc))

    private def currentRowGroupStatisticsMax =
      currentRowGroupStatistics.flatMap(statsMaxValue).map(value => decoder.decode(value, vcc))

    private def extremeOfRowGroup(currentExtremeOpt: Option[V], choose: (V, V) => V) = {
      val store = reader.readNextFilteredRowGroup()
      val recordReader =
        columnIO.getRecordReader(
          store,
          new ParquetRecordMaterializer(schema = requestedSchema, columnProjections = Seq.empty),
          filter.toFilterCompat(vcc)
        )
      (0L until store.getRowCount.longValue()).iterator
        .map(_ => Option(recordReader.read()))
        .collect { case Some(record) =>
          record.get(columnPath)
        }
        .collect {
          case Some(value) if value != NullValue => decoder.decode(value, vcc)
        }
        .foldLeft(currentExtremeOpt) {
          case (None, v)    => Some(v)
          case (Some(a), v) => Some(choose(a, v))
        }
    }

    @tailrec
    private def advance(
        currentExtremeOpt: Option[V],
        choose: (V, V)        => V,
        shouldSkipRowGroup: V => Boolean
    ): Option[V] =
      if (currentBlock >= numOfBlocks) {
        // end of file
        currentExtremeOpt
      } else if (currentExtremeOpt.isEmpty) {
        advance(extremeOfRowGroup(currentExtremeOpt, choose), choose, shouldSkipRowGroup)
      } else {
        advance(
          currentExtremeOpt.flatMap {
            case currentMin if shouldSkipRowGroup(currentMin) =>
              // according to stats we will not find better value in current rowGroup
              reader.skipNextRowGroup()
              currentExtremeOpt
            case _ =>
              extremeOfRowGroup(currentExtremeOpt, choose)
          },
          choose,
          shouldSkipRowGroup
        )
      }

    def min: Option[V] =
      advance(
        currentExtremeOpt = startExtremeOpt,
        choose            = ordering.min,
        shouldSkipRowGroup =
          currentMin => currentRowGroupStatisticsMin.exists(statMin => ordering.gteq(statMin, currentMin))
      )

    def max: Option[V] =
      advance(
        currentExtremeOpt = startExtremeOpt,
        choose            = ordering.max,
        shouldSkipRowGroup =
          currentMax => currentRowGroupStatisticsMax.exists(statMax => ordering.lteq(statMax, currentMax))
      )

  }

  override def recordCount: Long = {
    val reader = new RecordCountReader
    try reader.filteredRecordCount
    finally reader.close()
  }

  override def min[V](columnPath: ColumnPath, currentMin: Option[V])(implicit
      decoder: ValueDecoder[V],
      ordering: Ordering[V]
  ): Option[V] = {
    val reader = new MinMaxReader[V](columnPath, currentMin)
    try reader.min
    finally reader.close()
  }

  override def max[V](columnPath: ColumnPath, currentMax: Option[V])(implicit
      decoder: ValueDecoder[V],
      ordering: Ordering[V]
  ): Option[V] = {
    val reader = new MinMaxReader[V](columnPath, currentMax)
    try reader.max
    finally reader.close()
  }

}

private case class Mat[V](value: V) extends AnyVal

private class RowCountMaterializer(schema: MessageType) extends RecordMaterializer[Mat[Long]] {
  override val getCurrentRecord: Mat[Long]      = Mat(1L)
  override val getRootConverter: GroupConverter = new DummyGroupConverter(schema)
}

private object DummyPrimitiveConverter extends PrimitiveConverter {
  override def addInt(value: Int): Unit         = ()
  override def addFloat(value: Float): Unit     = ()
  override def addBinary(value: Binary): Unit   = ()
  override def addDouble(value: Double): Unit   = ()
  override def addLong(value: Long): Unit       = ()
  override def addBoolean(value: Boolean): Unit = ()
}
private class DummyGroupConverter(schema: GroupType) extends GroupConverter {
  override def getConverter(fieldIndex: Int): Converter = schema.getType(fieldIndex) match {
    case groupType: GroupType => new DummyGroupConverter(groupType)
    case _                    => DummyPrimitiveConverter
  }
  override def start(): Unit = ()
  override def end(): Unit   = ()
}
