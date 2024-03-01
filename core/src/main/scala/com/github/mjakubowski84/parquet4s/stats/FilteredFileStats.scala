package com.github.mjakubowski84.parquet4s.stats

import com.github.mjakubowski84.parquet4s.*
import org.apache.parquet.ParquetReadOptions
import org.apache.parquet.filter2.compat.FilterCompat
import org.apache.parquet.hadoop.ParquetFileReader
import org.apache.parquet.io.api.*
import org.apache.parquet.io.{ColumnIOFactory, InputFile, MessageColumnIO, RecordReader}
import org.apache.parquet.schema.{GroupType, MessageType}

import scala.annotation.tailrec
import scala.jdk.CollectionConverters.*
import scala.util.Using

/** Calculates statistics from <b>filtered</b> Parquet files.
  */
private[parquet4s] class FilteredFileStats(
    inputFile: InputFile,
    vcc: ValueCodecConfiguration,
    projectionSchemaOpt: Option[MessageType],
    filter: FilterCompat.Filter
) extends Stats {

  private val readerOptions = ParquetReadOptions.builder().withRecordFilter(filter).build()

  abstract private class StatsReader extends AutoCloseable {
    protected val reader: ParquetFileReader = ParquetFileReader.open(inputFile, readerOptions)
    projectionSchemaOpt.foreach(reader.setRequestedSchema)

    private val fileMetaData                   = reader.getFooter.getFileMetaData
    private val messageSchema                  = fileMetaData.getSchema
    protected val requestedSchema: MessageType = projectionSchemaOpt.getOrElse(messageSchema)
    private val columnIOFactory                = new ColumnIOFactory(fileMetaData.getCreatedBy)
    // strict type checking should come from Hadoop Configuration or readerOptions.isEnabled(STRICT_TYPE_CHECKING, true);
    // but we enforce it for now (true flag)
    protected val columnIO: MessageColumnIO = columnIOFactory.getColumnIO(requestedSchema, messageSchema, true)

    def close(): Unit = reader.close()
  }

  private class RecordCountReader extends StatsReader {
    private val rowCountMaterializer = new RowCountMaterializer(requestedSchema)
    def filteredRecordCount: Long =
      Iterator
        .continually(reader.readNextFilteredRowGroup())
        .takeWhile(_ != null)
        .flatMap { store =>
          val recordReader = columnIO.getRecordReader(store, rowCountMaterializer, filter)
          (0L until store.getRowCount.longValue()).iterator
            .map(_ => Option(recordReader.read())) // unmaterialised record counter, catching errors, etc
            .collect { case Some(v) if !recordReader.shouldSkipCurrentRecord => v.value }
        }
        .sum
  }

  private class MinMaxReader[V](columnPath: ColumnPath, startExtremeOpt: Option[V])(implicit
      decoder: ValueDecoder[V],
      ordering: Ordering[V]
  ) extends StatsReader {
    private val recordMaterializer =
      new ParquetRecordMaterializer(schema = requestedSchema, columnProjections = Seq.empty)
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
      extremeOfRowGroupRec(
        recordReader       = columnIO.getRecordReader(store, recordMaterializer, filter),
        currentRecordIndex = 0L,
        maxRecordIndex     = store.getRowCount.longValue() - 1,
        currentExtremeOpt  = currentExtremeOpt,
        choose             = choose
      )
      // TODO should catch any RuntimeException and throw ParquetDecodingException
    }

    @tailrec
    private def extremeOfRowGroupRec(
        recordReader: RecordReader[RowParquetRecord],
        currentRecordIndex: Long,
        maxRecordIndex: Long,
        currentExtremeOpt: Option[V],
        choose: (V, V) => V
    ): Option[V] = {
      val recordValue = recordReader.read() // TODO unmaterializableRecordCounter or similar
      val newExtremeOpt = if (recordValue != null && !recordReader.shouldSkipCurrentRecord) {
        val columnValueOpt = recordValue.get(columnPath).filter(_ != NullValue).map(value => decoder.decode(value, vcc))
        (currentExtremeOpt, columnValueOpt) match {
          case (None, v)          => v
          case (Some(a), Some(v)) => Some(choose(a, v))
          case (a, None)          => a
        }
      } else {
        currentExtremeOpt
      }
      if (currentRecordIndex >= maxRecordIndex) newExtremeOpt
      else extremeOfRowGroupRec(recordReader, currentRecordIndex + 1, maxRecordIndex, newExtremeOpt, choose)
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

  override def recordCount: Long =
    Using.resource(new RecordCountReader)(_.filteredRecordCount)

  override def min[V](columnPath: ColumnPath, currentMin: Option[V])(implicit
      decoder: ValueDecoder[V],
      ordering: Ordering[V]
  ): Option[V] =
    Using.resource(new MinMaxReader[V](columnPath, currentMin))(_.min)

  override def max[V](columnPath: ColumnPath, currentMax: Option[V])(implicit
      decoder: ValueDecoder[V],
      ordering: Ordering[V]
  ): Option[V] =
    Using.resource(new MinMaxReader[V](columnPath, currentMax))(_.max)

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
