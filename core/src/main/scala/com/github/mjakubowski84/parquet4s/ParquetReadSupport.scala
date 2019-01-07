package com.github.mjakubowski84.parquet4s

import java.math.BigInteger
import java.util

import org.apache.hadoop.conf.Configuration
import org.apache.parquet.hadoop.api.{InitContext, ReadSupport}
import org.apache.parquet.io.api._
import org.apache.parquet.schema._

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

sealed trait ParquetRecord extends Value {

  type Self

  def add(name: String, value: Value): Self

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

object RowParquetRecord {

  def apply(entries : (String, Value)*): RowParquetRecord =
    entries.foldLeft(new RowParquetRecord()) {
      case (record, (key, value)) => record.add(key, value)
    }

}

class RowParquetRecord private extends ParquetRecord {

  override type Self = this.type

  private val values = ArrayBuffer.empty[(String, Value)]

  override def add(name: String, value: Value): Self = {
    values.append((name, value))
    this
  }

  def getMap: Map[String, Value] = values.toMap

  override def toString: String =
    values
      .map { case (key, value) => s"$key=$value"}
      .mkString(getClass.getSimpleName + " (", ",", ")")

  def prepend(name: String, value: Value): RowParquetRecord = { // TODO add or prepend?
    values.prepend((name, value))
    this
  }

  override def write(schema: Type, recordConsumer: RecordConsumer): Unit = {
    val groupSchema = schema.asGroupType()
    recordConsumer.startGroup()
    values.foreach {
      case (_, NullValue) =>
        // TODO write a test for writing case class with null and option field
      case (name, value) =>
        val index = groupSchema.getFieldIndex(name)
        recordConsumer.startField(name, index)
        value.write(groupSchema.getType(name), recordConsumer)
        recordConsumer.endField(name, index)
    }
    recordConsumer.endGroup()
  }

}

object ListParquetRecord {

  private val ListFieldName = "list"
  private val ElementFieldName = "element"

  def apply(elements: Value*): ListParquetRecord =
    elements.foldLeft(new ListParquetRecord()) {
      case (record, element) => record.add(ListFieldName, RowParquetRecord(ElementFieldName -> element))
    }

}

class ListParquetRecord private extends ParquetRecord {
  import ListParquetRecord._

  override type Self = this.type

  private val values = ArrayBuffer.empty[Value]

  override def add(name: String, value: Value): Self = {
    // name should always be equal to ListFieldName
    val element = value.asInstanceOf[RowParquetRecord].getMap(ElementFieldName)
    values.append(element)
    this
  }

  def elements: List[Value] = values.toList // TODO let's use vector, check if ArrayBuffer is a good choice

  override def toString: String = values.mkString(getClass.getSimpleName + " (", ",", ")")

  override def write(schema: Type, recordConsumer: RecordConsumer): Unit = {
    val groupSchema = schema.asGroupType()
    val listSchema = groupSchema.getType(ListFieldName).asGroupType()
    val listIndex = groupSchema.getFieldIndex(ListFieldName)
    val elementIndex = listSchema.getFieldIndex(ElementFieldName)

    recordConsumer.startGroup()

    if (!isEmpty) {
      recordConsumer.startField(ListFieldName, listIndex)
      recordConsumer.startGroup()
      recordConsumer.startField(ElementFieldName, elementIndex)

      values.foreach {
        case NullValue =>
        // TODO write a test for writing collection with null element and optional element, and collection with only null elements!
        case value =>
          value.write(listSchema.getType(ElementFieldName), recordConsumer)
      }

      recordConsumer.endField(ElementFieldName, elementIndex)
      recordConsumer.endGroup()
      recordConsumer.endField(ListFieldName, listIndex)
    }

    recordConsumer.endGroup()
  }

  private def isEmpty: Boolean = values.isEmpty || values.forall(_ == NullValue)
}

class ListParquetRecordConverter(schema: GroupType, name: String, parent: ParquetRecordConverter[_ <: ParquetRecord])
  extends ParquetRecordConverter[ListParquetRecord](schema, Option(name), Option(parent)){

  override def start(): Unit = {
    this.record = ListParquetRecord()
  }

}

object MapParquetRecord {

  def apply(entries : (Value, Value)*): MapParquetRecord = {
    entries.foldLeft(new MapParquetRecord()) {
      case (record, (key, value)) => record.add("key_value", RowParquetRecord("key" -> key, "value" -> value)) // TODO
    }
  }

}

class MapParquetRecord private extends ParquetRecord {

  override type Self = this.type

  private val values = scala.collection.mutable.Map.empty[Value, Value]

  override def add(name: String, value: Value): Self = {
    val keyValueRecord = value.asInstanceOf[RowParquetRecord]
    val mapKey = keyValueRecord.getMap("key")
    val mapValue = keyValueRecord.getMap("value")
    values.put(mapKey, mapValue)
    this
  }

  def getMap: Map[Value, Value] = values.toMap

  override def toString: String =
    values
      .map { case (key, value) => s"$key=$value"}
      .mkString(getClass.getSimpleName + " (", ",", ")")

  override def write(schema: Type, recordConsumer: RecordConsumer): Unit = ??? // TODO
}

class MapParquetRecordConverter(schema: GroupType, name: String, parent: ParquetRecordConverter[_ <: ParquetRecord])
  extends ParquetRecordConverter[MapParquetRecord](schema, Option(name), Option(parent)) {

  override def start(): Unit = {
    this.record = MapParquetRecord()
  }

}
