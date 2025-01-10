package com.github.mjakubowski84.parquet4s

import org.apache.hadoop.conf.Configuration
import org.apache.parquet.hadoop.api.{InitContext, ReadSupport}
import org.apache.parquet.io.api.*
import org.apache.parquet.schema.*
import org.apache.parquet.schema.LogicalTypeAnnotation.{
  DecimalLogicalTypeAnnotation,
  ListLogicalTypeAnnotation,
  MapLogicalTypeAnnotation,
  TimestampLogicalTypeAnnotation
}

import scala.jdk.CollectionConverters.*
import java.math.BigInteger
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName
import org.apache.parquet.io.ParquetDecodingException

private[parquet4s] class ParquetReadSupport(
    projectedSchemaOpt: Option[MessageType],
    columnProjections: Seq[ColumnProjection],
    metadataReader: MetadataReader
) extends ReadSupport[RowParquetRecord] {

  override def prepareForRead(
      configuration: Configuration,
      keyValueMetaData: java.util.Map[String, String],
      fileSchema: MessageType,
      readContext: ReadSupport.ReadContext
  ): RecordMaterializer[RowParquetRecord] = {
    metadataReader.setMetadata(keyValueMetaData.asScala)
    new ParquetRecordMaterializer(readContext.getRequestedSchema, columnProjections)
  }

  override def init(context: InitContext): ReadSupport.ReadContext =
    new ReadSupport.ReadContext(projectedSchemaOpt.foldLeft(context.getFileSchema)(ReadSupport.getSchemaForRead))
}

private[parquet4s] class ParquetRecordMaterializer(
    schema: MessageType,
    columnProjections: Seq[ColumnProjection]
) extends RecordMaterializer[RowParquetRecord] {

  private val root = new RootRowParquetRecordConverter(schema, columnProjections)

  override def getCurrentRecord: RowParquetRecord = root.getCurrentRecord

  override def getRootConverter: GroupConverter = root

}

abstract private class ParquetRecordConverter[R <: ParquetRecord[?, R]](schema: GroupType) extends GroupConverter {

  protected var record: R = _

  private val converters: List[Converter] = schema.getFields.asScala.toList.map(createConverter)

  private def createConverter(field: Type): Converter = {

    val fieldName = field.getName

    Option(field.getLogicalTypeAnnotation) match {
      case Some(ann: DecimalLogicalTypeAnnotation) if field.isPrimitive =>
        val primitiveType = field.asPrimitiveType
        primitiveType.getPrimitiveTypeName match {
          case PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY =>
            new DecimalConverter(
              name = fieldName,
              format = new DecimalFormat.BinaryFormat(
                scale           = ann.getScale(),
                precision       = ann.getPrecision(),
                byteArrayLength = primitiveType.getTypeLength(),
                rescaleOnRead   = false // converter is not respomsible for rescaling
              )
            )
          case PrimitiveTypeName.INT32 =>
            new DecimalConverter(
              name = fieldName,
              format = new DecimalFormat.IntFormat(
                scale         = ann.getScale(),
                precision     = ann.getPrecision(),
                rescaleOnRead = false // converter is not respomsible for rescaling
              )
            )
          case PrimitiveTypeName.INT64 =>
            new DecimalConverter(
              name = fieldName,
              format = new DecimalFormat.LongFormat(
                scale         = ann.getScale(),
                precision     = ann.getPrecision(),
                rescaleOnRead = false // converter is not respomsible for rescaling
              )
            )
          case other =>
            throw new ParquetDecodingException(s"$other is unsupported as a decimal type")
        }
      case Some(ann: TimestampLogicalTypeAnnotation) =>
        new DateTimeConverter(name = fieldName, timeUnit = ann.getUnit)
      case _ if field.isPrimitive =>
        new ParquetPrimitiveConverter(fieldName)
      case Some(_: MapLogicalTypeAnnotation) =>
        new MapParquetRecordConverter(field.asGroupType(), fieldName, parent = this)
      case Some(_: ListLogicalTypeAnnotation) =>
        new ListParquetRecordConverter(field.asGroupType(), fieldName, parent = this)
      case _ =>
        new ChildRowParquetRecordConverter(field.asGroupType(), fieldName, parent = this)
    }
  }

  override def getConverter(fieldIndex: Int): Converter = converters(fieldIndex)

  def getCurrentRecord: R = record

  def update(name: String, value: Value): Unit =
    this.record = this.record.add(name, value)

  private class ParquetPrimitiveConverter(name: String) extends PrimitiveConverter {
    override def addBinary(value: Binary): Unit =
      record = record.add(name, BinaryValue(value))

    override def addBoolean(value: Boolean): Unit =
      record = record.add(name, BooleanValue(value))

    override def addDouble(value: Double): Unit =
      record = record.add(name, DoubleValue(value))

    override def addFloat(value: Float): Unit =
      record = record.add(name, FloatValue(value))

    override def addInt(value: Int): Unit =
      record = record.add(name, IntValue(value))

    override def addLong(value: Long): Unit =
      record = record.add(name, LongValue(value))
  }

  private class DecimalConverter(name: String, format: DecimalFormat.Format) extends ParquetPrimitiveConverter(name) {

    override def addBinary(value: Binary): Unit =
      record = record.add(name, DecimalValue(new BigInteger(value.getBytes()), format))

    override def addLong(value: Long): Unit =
      record = record.add(name, DecimalValue(BigInteger.valueOf(value), format))

    override def addInt(value: Int): Unit =
      record = record.add(name, DecimalValue(BigInteger.valueOf(value.toLong), format))
  }

  private class DateTimeConverter(name: String, timeUnit: LogicalTypeAnnotation.TimeUnit)
      extends ParquetPrimitiveConverter(name) {
    private val format = timeUnit match {
      case LogicalTypeAnnotation.TimeUnit.MILLIS =>
        TimestampFormat.Int64Millis
      case LogicalTypeAnnotation.TimeUnit.MICROS =>
        TimestampFormat.Int64Micros
      case LogicalTypeAnnotation.TimeUnit.NANOS =>
        TimestampFormat.Int64Nanos
    }

    override def addLong(value: Long): Unit =
      record = record.add(name, DateTimeValue(value, format))
  }

}

abstract private class RowParquetRecordConverter(schema: GroupType)
    extends ParquetRecordConverter[RowParquetRecord](schema) {

  /* Initial record has all fields (according to the schema) set with NullValues.
     During reading those nulls are replaced with a real value from a file.
     Missing values stay null.
     Thanks to that generic record preserves null representation for missing values.
   */
  private lazy val initial = RowParquetRecord.emptyWithSchema(
    schema.getFields.asScala.map(_.getName)
  )

  override def start(): Unit = record = initial

}

private class RootRowParquetRecordConverter(schema: GroupType, columnProjections: Seq[ColumnProjection])
    extends RowParquetRecordConverter(schema) {

  private lazy val emptyProjectionRow =
    RowParquetRecord.emptyWithSchema(columnProjections.map(cp => cp.alias.getOrElse(cp.columnPath.elements.last)))

  override def end(): Unit =
    if (columnProjections.nonEmpty) {
      record = columnProjections.foldLeft(emptyProjectionRow) {
        case (newRecord, ColumnProjection(columnPath, _, aliasOpt)) =>
          record.get(columnPath) match {
            case Some(value) =>
              newRecord.add(aliasOpt.getOrElse(columnPath.elements.last), value)
            case None =>
              throw new IllegalArgumentException(s"""Invalid column projection: "$columnPath".""")
          }
      }
    }

}

private class ChildRowParquetRecordConverter(
    schema: GroupType,
    name: String,
    parent: ParquetRecordConverter[? <: ParquetRecord[?, ?]]
) extends RowParquetRecordConverter(schema) {

  override def end(): Unit = parent.update(name, record)

}

private class ListParquetRecordConverter[P <: ParquetRecord[?, P]](
    schema: GroupType,
    name: String,
    parent: ParquetRecordConverter[P]
) extends ParquetRecordConverter[ListParquetRecord](schema) {

  override def start(): Unit =
    this.record = ListParquetRecord.Empty

  override def end(): Unit = parent.update(name, record)

}

private class MapParquetRecordConverter[P <: ParquetRecord[?, P]](
    schema: GroupType,
    name: String,
    parent: ParquetRecordConverter[P]
) extends ParquetRecordConverter[MapParquetRecord](schema) {

  override def start(): Unit =
    this.record = MapParquetRecord.Empty

  override def end(): Unit = parent.update(name, record)

}
