package com.github.mjakubowski84.parquet4s

import java.math.MathContext
import java.util
import org.apache.hadoop.conf.Configuration
import org.apache.parquet.hadoop.api.{InitContext, ReadSupport}
import org.apache.parquet.io.api._
import org.apache.parquet.schema.LogicalTypeAnnotation.{DecimalLogicalTypeAnnotation, IntLogicalTypeAnnotation, ListLogicalTypeAnnotation, MapLogicalTypeAnnotation, StringLogicalTypeAnnotation}
import org.apache.parquet.schema._

import scala.jdk.CollectionConverters._


private[parquet4s] class ParquetReadSupport(projectedSchemaOpt: Option[MessageType] = None)
  extends ReadSupport[RowParquetRecord] {

  override def prepareForRead(
                               configuration: Configuration,
                               keyValueMetaData: util.Map[String, String],
                               fileSchema: MessageType,
                               readContext: ReadSupport.ReadContext
                             ): RecordMaterializer[RowParquetRecord] =
    new ParquetRecordMaterializer(readContext.getRequestedSchema)

  override def init(context: InitContext): ReadSupport.ReadContext =
    new ReadSupport.ReadContext(projectedSchemaOpt.foldLeft(context.getFileSchema)(ReadSupport.getSchemaForRead))

}

private[parquet4s] class ParquetRecordMaterializer(schema: MessageType) extends RecordMaterializer[RowParquetRecord] {

  private val root = new RowParquetRecordConverter(schema)

  override def getCurrentRecord: RowParquetRecord = root.getCurrentRecord

  override def getRootConverter: GroupConverter = root

}

private abstract class ParquetRecordConverter[R <: ParquetRecord[_, R]](
                                                                     schema: GroupType,
                                                                     name: Option[String],
                                                                     parent: Option[ParquetRecordConverter[_ <: ParquetRecord[_, _]]]
                                                                   ) extends GroupConverter {

  protected var record: R = _

  private val converters: List[Converter] = schema.getFields.asScala.toList.map(createConverter)

  private def createConverter(field: Type): Converter = {
    Option(field.getLogicalTypeAnnotation) match {
      case Some(_ : StringLogicalTypeAnnotation) =>
        new StringConverter(field.getName)
      case Some(ann : DecimalLogicalTypeAnnotation) =>
        new DecimalConverter(
          name = field.getName, 
          scale = ann.getScale,
          precision = ann.getPrecision
        )
      case Some(ann: IntLogicalTypeAnnotation) if ann.getBitWidth == 8 =>
        new ByteConverter(field.getName)
      case Some(ann: IntLogicalTypeAnnotation) if ann.getBitWidth == 16 =>
        new ShortConverter(field.getName)
      case _ if field.isPrimitive =>
        new ParquetPrimitiveConverter(field.getName)
      case Some(_: MapLogicalTypeAnnotation) =>
        new MapParquetRecordConverter(field.asGroupType(), field.getName, this)
      case Some(_: ListLogicalTypeAnnotation) =>
        new ListParquetRecordConverter(field.asGroupType(), field.getName, this)
      case _ =>
        new RowParquetRecordConverter(field.asGroupType(), Option(field.getName), Some(this))
    }
  }

  override def getConverter(fieldIndex: Int): Converter = converters(fieldIndex)

  def getCurrentRecord: R = record

  protected def update(name: String, value: Value): Unit =
    this.record = this.record.add(name, value)

  override def end(): Unit = {
    parent.foreach(_.update(name.get, record))
  }

  private class ParquetPrimitiveConverter(name: String) extends PrimitiveConverter {
    override def addBinary(value: Binary): Unit = {
      record = record.add(name, BinaryValue(value))
    }

    override def addBoolean(value: Boolean): Unit = {
      record = record.add(name, BooleanValue(value))
    }

    override def addDouble(value: Double): Unit = {
      record = record.add(name, DoubleValue(value))
    }

    override def addFloat(value: Float): Unit = {
      record = record.add(name, FloatValue(value))
    }

    override def addInt(value: Int): Unit = {
      record = record.add(name, IntValue(value))
    }

    override def addLong(value: Long): Unit = {
      record = record.add(name, LongValue(value))
    }
  }

  private class StringConverter(name: String) extends ParquetPrimitiveConverter(name) {
    override def addBinary(value: Binary): Unit = {
      record = record.add(name, BinaryValue(value))
    }
  }

  private class ShortConverter(name: String) extends ParquetPrimitiveConverter(name) {
    override def addInt(value: Int): Unit = {
      record = record.add(name, IntValue(value))
    }
  }

  private class ByteConverter(name: String) extends ParquetPrimitiveConverter(name) {
    override def addInt(value: Int): Unit = {
      record = record.add(name, IntValue(value))
    }
  }

  private class DecimalConverter(name: String, scale: Int, precision: Int) extends ParquetPrimitiveConverter(name) {
    private lazy val mathContext = new MathContext(precision)
    private val shouldRescale = scale != Decimals.Scale || precision != Decimals.Precision

    override def addBinary(value: Binary): Unit = {
      val rescaled =
        if (shouldRescale) Decimals.rescaleBinary(value, scale, mathContext)
        else value
      record = record.add(name, BinaryValue(rescaled))
    }
  }

}

private class RowParquetRecordConverter(schema: GroupType, name: Option[String], parent: Option[ParquetRecordConverter[_ <: ParquetRecord[_, _]]])
  extends ParquetRecordConverter[RowParquetRecord](schema, name, parent) {

  /* Initial record has all fields (according to schema) set with NullValues.
     During reading those nulls are replaced with a real value from a file.
     Missing values stay null.
     Thanks to that generic record preserves null representation for missing values.
   */
  private lazy val initial = RowParquetRecord.emptyWithSchema(schema.getFields.asScala.map(_.getName))

  def this(schema: GroupType) = {
    this(schema, None, None)
  }

  override def start(): Unit = {
    record = initial
  }
  
}

private class ListParquetRecordConverter[P <: ParquetRecord[_, P]](schema: GroupType, name: String, parent: ParquetRecordConverter[P])
  extends ParquetRecordConverter[ListParquetRecord](schema, Option(name), Option(parent)){

  override def start(): Unit = {
    this.record = ListParquetRecord.Empty
  }

}

private class MapParquetRecordConverter[P <: ParquetRecord[_, P]](schema: GroupType, name: String, parent: ParquetRecordConverter[P])
  extends ParquetRecordConverter[MapParquetRecord](schema, Option(name), Option(parent)) {

  override def start(): Unit = {
    this.record = MapParquetRecord.Empty
  }

}
