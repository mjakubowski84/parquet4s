package com.github.mjakubowski84.parquet4s

import org.apache.hadoop.conf.Configuration
import org.apache.parquet.hadoop.api.{InitContext, ReadSupport}
import org.apache.parquet.io.api.*
import org.apache.parquet.schema.*
import org.apache.parquet.schema.LogicalTypeAnnotation.{
  DecimalLogicalTypeAnnotation,
  ListLogicalTypeAnnotation,
  MapLogicalTypeAnnotation
}

import java.math.MathContext
import java.util
import scala.jdk.CollectionConverters.*

private[parquet4s] class ParquetReadSupport(
    projectedSchemaOpt: Option[MessageType]  = None,
    columnProjections: Seq[ColumnProjection] = Seq.empty
) extends ReadSupport[RowParquetRecord] {

  override def prepareForRead(
      configuration: Configuration,
      keyValueMetaData: util.Map[String, String],
      fileSchema: MessageType,
      readContext: ReadSupport.ReadContext
  ): RecordMaterializer[RowParquetRecord] =
    new ParquetRecordMaterializer(readContext.getRequestedSchema, columnProjections)

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
      case Some(ann: DecimalLogicalTypeAnnotation) =>
        new DecimalConverter(
          name      = fieldName,
          scale     = ann.getScale,
          precision = ann.getPrecision
        )
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

  private class DecimalConverter(name: String, scale: Int, precision: Int) extends ParquetPrimitiveConverter(name) {
    private lazy val mathContext = new MathContext(precision)
    private val shouldRescale    = scale != Decimals.Scale || precision != Decimals.Precision

    override def addBinary(value: Binary): Unit = {
      val rescaled =
        if (shouldRescale) Decimals.rescaleBinary(value, scale, mathContext)
        else value
      record = record.add(name, BinaryValue(rescaled))
    }
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

  override def end(): Unit =
    columnProjections.foreach { case ColumnProjection(columnPath, ordinal, aliasOpt) =>
      record.get(columnPath) match {
        case Some(value) if columnPath.elements.length > 1 =>
          record = record.updated(ordinal, aliasOpt.getOrElse(columnPath.elements.last), value)
        case Some(_) =>
          aliasOpt.foreach { alias =>
            record = record.rename(ordinal, alias)
          }
        case None =>
          throw new IllegalArgumentException(s"""Invalid column projection: "$columnPath".""")
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
