package com.mjakubowski84

import java.math.BigInteger
import java.util

import org.apache.hadoop.conf.Configuration
import org.apache.parquet.hadoop.api.{InitContext, ReadSupport}
import org.apache.parquet.io.api._
import org.apache.parquet.schema.{GroupType, MessageType, OriginalType, Type}

import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer


class ParquetReadSupport extends ReadSupport[ParquetRecord] {

  override def prepareForRead(configuration: Configuration, keyValueMetaData: util.Map[String, String], fileSchema: MessageType, readContext: ReadSupport.ReadContext): RecordMaterializer[ParquetRecord] =
    new ParquetRecordMaterializer(fileSchema)

  override def init(context: InitContext): ReadSupport.ReadContext = new ReadSupport.ReadContext(context.getFileSchema)

}


class ParquetRecordMaterializer(schema: MessageType) extends RecordMaterializer[ParquetRecord] {

  private val root = new ParquetRecordConverter(schema)

  override def getCurrentRecord: ParquetRecord = root.getCurrentRecord

  override def getRootConverter: GroupConverter = root

}

// TODO maybe it would be better if we have parquet-row-record converter and abstract ParquetRecordConverter?
class ParquetRecordConverter(schema: GroupType, name: Option[String], parent: Option[ParquetRecordConverter]) extends GroupConverter {

  def this(schema: GroupType) {
    this(schema, None, None)
  }

  private val converters: List[Converter] = schema.getFields.asScala.toList.map(createConverter)
  protected var record: ParquetRecord = _ // TODO probably var is not needed, maybe start can be empty, check with test with multi row data

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
        new ParquetRecordConverter(field.asGroupType(), Option(field.getName), Some(this))

    }
  }

  override def getConverter(fieldIndex: Int): Converter = converters(fieldIndex)

  def getCurrentRecord: ParquetRecord = record

  override def start(): Unit = {
    record = new ParquetRowRecord()
  }

  override def end(): Unit = {
    parent.foreach(_.getCurrentRecord.add(name.get, record))
  }

  private class ParquetPrimitiveConverter(name: String) extends PrimitiveConverter {
    override def addBinary(value: Binary): Unit = {
      record.add(name, value.getBytes)
    }

    override def addBoolean(value: Boolean): Unit = {
      record.add(name, value)
    }

    override def addDouble(value: Double): Unit = {
      record.add(name, value)
    }

    override def addFloat(value: Float): Unit = {
      record.add(name, value)
    }

    override def addInt(value: Int): Unit = {
      record.add(name, value)
    }

    override def addLong(value: Long): Unit = {
      record.add(name, value)
    }
  }

  private class StringConverter(name: String) extends ParquetPrimitiveConverter(name) {
    override def addBinary(value: Binary): Unit = {
      record.add(name, value.toStringUsingUTF8)
    }
  }

  private class DecimalConverter(name: String, scale: Int) extends ParquetPrimitiveConverter(name) {
    override def addBinary(value: Binary): Unit = {
      record.add(name, BigDecimal(new BigInteger(value.getBytes), scale))
    }
  }

}

trait ParquetRecord {

  def add(name: String, value: Any): Unit

  override def toString: String

  def toObject[T: MapReader]: T = throw new NotImplementedError(s"Object creation impossible for ${this.getClass.getSimpleName}")

}

class ParquetRowRecord extends ParquetRecord {

  private val values = ArrayBuffer.empty[(String, Any)]

  def add(name: String, value: Any): Unit = {
    values.append((name, value))
  }

  def getMap: Map[String, Any] = values.toMap

  override def toString: String = getMap.toString

  override def toObject[T: MapReader]: T = MapReader.apply[T].read(getMap) // will not be needed if we read record not map

}

class ListParquetRecord extends ParquetRecord {
  private val values = ArrayBuffer.empty[Any]

  override def add(name: String, value: Any): Unit = {
    val element = value.asInstanceOf[ParquetRowRecord].getMap("element")
    values.append(element)
  }

  override def toString: String = values.toString()

  def getList: List[Any] = values.toList
}

class ListParquetRecordConverter(schema: GroupType, name: String, parent: ParquetRecordConverter)
  extends ParquetRecordConverter(schema, Option(name), Option(parent)){

  override def start(): Unit = {
    this.record = new ListParquetRecord()
  }

}

class MapParquetRecord extends ParquetRecord {

  private val values = ArrayBuffer.empty[(Any, Any)]

  override def add(name: String, value: Any): Unit = {
    val keyValueRecord = value.asInstanceOf[ParquetRowRecord]
    val mapKey = keyValueRecord.getMap("key")
    val mapValue = keyValueRecord.getMap("value")
    values.append((mapKey, mapValue))
  }

  def getMap: Map[Any, Any] = values.toMap

  override def toString: String = getMap.toString()

}

class MapParquetRecordConverter(schema: GroupType, name: String, parent: ParquetRecordConverter)
  extends ParquetRecordConverter(schema, Option(name), Option(parent)) {

  override def start(): Unit = {
    this.record = new MapParquetRecord()
  }

}
