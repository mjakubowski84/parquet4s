package com.github.mjakubowski84.parquet4s

import java.util

import org.apache.hadoop.conf.Configuration
import org.apache.parquet.hadoop.api.{InitContext, ReadSupport}
import org.apache.parquet.io.api._
import org.apache.parquet.schema._

import scala.collection.JavaConverters._


class ParquetReadSupport extends ReadSupport[RowParquetRecord] {

  override def prepareForRead(
                               configuration: Configuration,
                               keyValueMetaData: util.Map[String, String],
                               fileSchema: MessageType,
                               readContext: ReadSupport.ReadContext
                             ): RecordMaterializer[RowParquetRecord] =
    new ParquetRecordMaterializer(fileSchema)

  override def init(context: InitContext): ReadSupport.ReadContext = new ReadSupport.ReadContext(context.getFileSchema)

}

class ParquetRecordMaterializer(schema: MessageType) extends RecordMaterializer[RowParquetRecord] {

  private val root = new RowParquetRecordConverter(schema)

  override def getCurrentRecord: RowParquetRecord = root.getCurrentRecord

  override def getRootConverter: GroupConverter = root

}

abstract class ParquetRecordConverter[R <: ParquetRecord](
                                                           schema: GroupType,
                                                           name: Option[String],
                                                           parent: Option[ParquetRecordConverter[_ <: ParquetRecord]]
                                                         ) extends GroupConverter {

  protected var record: R = _

  private val converters: List[Converter] = schema.getFields.asScala.toList.map(createConverter)

  private def createConverter(field: Type): Converter = {
    Option(field.getOriginalType) match {
      case Some(OriginalType.UTF8) if field.isPrimitive =>
        new StringConverter(field.getName)
      case Some(OriginalType.DECIMAL) if field.isPrimitive =>
        val scale = field.asPrimitiveType.getDecimalMetadata.getScale
        new DecimalConverter(field.getName, scale)
      case _ if field.isPrimitive =>
        new ParquetPrimitiveConverter(field.getName)
      case Some(OriginalType.MAP) =>
        new MapParquetRecordConverter(field.asGroupType(), field.getName, this)
      case Some(OriginalType.LIST) =>
        new ListParquetRecordConverter(field.asGroupType(), field.getName, this)
      case _ =>
        new RowParquetRecordConverter(field.asGroupType(), Option(field.getName), Some(this))

    }
  }

  override def getConverter(fieldIndex: Int): Converter = converters(fieldIndex)

  def getCurrentRecord: R = record

  override def end(): Unit = {
    parent.foreach(_.getCurrentRecord.add(name.get, record))
  }

  private class ParquetPrimitiveConverter(name: String) extends PrimitiveConverter {
    override def addBinary(value: Binary): Unit = {
      record.add(name, new BinaryValue(value))
    }

    override def addBoolean(value: Boolean): Unit = {
      record.add(name, BooleanValue(value))
    }

    override def addDouble(value: Double): Unit = {
      record.add(name, DoubleValue(value))
    }

    override def addFloat(value: Float): Unit = {
      record.add(name, FloatValue(value))
    }

    override def addInt(value: Int): Unit = {
      record.add(name, IntValue(value))
    }

    override def addLong(value: Long): Unit = {
      record.add(name, LongValue(value))
    }
  }

  private class StringConverter(name: String) extends ParquetPrimitiveConverter(name) {
    override def addBinary(value: Binary): Unit = {
      record.add(name, new StringValue(value))
    }
  }

  private class DecimalConverter(name: String, scale: Int) extends ParquetPrimitiveConverter(name) {
    override def addBinary(value: Binary): Unit = ???
    /*
      TODO probably record should have pointer to schema, and real conversion should take place in codec

      record.add(name, new GenericValue(BigDecimal(new BigInteger(value.getBytes), scale))) // TODO hey, we do not have codecs for big decimal!!!
     */

  }

}

class RowParquetRecordConverter(schema: GroupType, name: Option[String], parent: Option[ParquetRecordConverter[_ <: ParquetRecord]])
  extends ParquetRecordConverter[RowParquetRecord](schema, name, parent) {

  def this(schema: GroupType) {
    this(schema, None, None)
  }

  override def start(): Unit = {
    record = RowParquetRecord()
  }
  
}

class ListParquetRecordConverter(schema: GroupType, name: String, parent: ParquetRecordConverter[_ <: ParquetRecord])
  extends ParquetRecordConverter[ListParquetRecord](schema, Option(name), Option(parent)){

  override def start(): Unit = {
    this.record = ListParquetRecord()
  }

}

class MapParquetRecordConverter(schema: GroupType, name: String, parent: ParquetRecordConverter[_ <: ParquetRecord])
  extends ParquetRecordConverter[MapParquetRecord](schema, Option(name), Option(parent)) {

  override def start(): Unit = {
    this.record = MapParquetRecord()
  }

}
