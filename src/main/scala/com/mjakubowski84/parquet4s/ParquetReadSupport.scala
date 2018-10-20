package com.mjakubowski84.parquet4s

import java.math.BigInteger
import java.util

import org.apache.hadoop.conf.Configuration
import org.apache.parquet.hadoop.api.{InitContext, ReadSupport}
import org.apache.parquet.io.api._
import org.apache.parquet.schema.{GroupType, MessageType, OriginalType, Type}

import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer


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

sealed trait ParquetRecord {

  type Self

  def add(name: String, value: Any): Self

  override def toString: String

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

class RowParquetRecordConverter(schema: GroupType, name: Option[String], parent: Option[ParquetRecordConverter[_ <: ParquetRecord]])
  extends ParquetRecordConverter[RowParquetRecord](schema, name, parent) {

  def this(schema: GroupType) {
    this(schema, None, None)
  }

  override def start(): Unit = {
    record = RowParquetRecord()
  }
  
}

object RowParquetRecord {

  def apply(entries : (String, Any)*): RowParquetRecord =
    entries.foldLeft(new RowParquetRecord()) {
      case (record, (key, value)) => record.add(key, value)
    }

}

class RowParquetRecord private extends ParquetRecord {

  override type Self = this.type

  private val values = ArrayBuffer.empty[(String, Any)]

  override def add(name: String, value: Any): Self = {
    values.append((name, value))
    this
  }

  def getMap: Map[String, Any] = values.toMap

  override def toString: String =
    values
      .map { case (key, value) => s"$key=$value"}
      .mkString(getClass.getSimpleName + " (", ",", ")")

}

object ListParquetRecord {

  def apply(elements: Any*): ListParquetRecord =
    elements.foldLeft(new ListParquetRecord()) {
      case (record, element) => record.add("list", RowParquetRecord("element" -> element))
    }

}

class ListParquetRecord private extends ParquetRecord {

  override type Self = this.type

  private val values = ArrayBuffer.empty[Any]

  override def add(name: String, value: Any): Self = {
    val element = value.asInstanceOf[RowParquetRecord].getMap("element")
    values.append(element)
    this
  }

  def getList: List[Any] = values.toList

  override def toString: String = values.mkString(getClass.getSimpleName + " (", ",", ")")

}

class ListParquetRecordConverter(schema: GroupType, name: String, parent: ParquetRecordConverter[_ <: ParquetRecord])
  extends ParquetRecordConverter[ListParquetRecord](schema, Option(name), Option(parent)){

  override def start(): Unit = {
    this.record = ListParquetRecord()
  }

}

object MapParquetRecord {

  def apply(entries : (String, Any)*): MapParquetRecord = {
    entries.foldLeft(new MapParquetRecord()) {
      case (record, (key, value)) => record.add("key_value", RowParquetRecord("key" -> key, "value" -> value))
    }
  }

}

class MapParquetRecord private extends ParquetRecord {

  override type Self = this.type

  private val values = scala.collection.mutable.Map.empty[Any, Any]

  override def add(name: String, value: Any): Self = {
    val keyValueRecord = value.asInstanceOf[RowParquetRecord]
    val mapKey = keyValueRecord.getMap("key")
    val mapValue = keyValueRecord.getMap("value")
    values.put(mapKey, mapValue)
    this
  }

  def getMap: Map[Any, Any] = values.toMap

  override def toString: String =
    values
      .map { case (key, value) => s"$key=$value"}
      .mkString(getClass.getSimpleName + " (", ",", ")")

}

class MapParquetRecordConverter(schema: GroupType, name: String, parent: ParquetRecordConverter[_ <: ParquetRecord])
  extends ParquetRecordConverter[MapParquetRecord](schema, Option(name), Option(parent)) {

  override def start(): Unit = {
    this.record = MapParquetRecord()
  }

}
