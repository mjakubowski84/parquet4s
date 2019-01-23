package com.github.mjakubowski84.parquet4s

import org.apache.parquet.io.api.RecordConsumer
import org.apache.parquet.schema.Type

import scala.collection.mutable.ArrayBuffer

sealed trait ParquetRecord extends Value {

  type Self

  def add(name: String, value: Value): Self

  override def toString: String

}

object RowParquetRecord {

  def apply(entries : (String, Value)*): RowParquetRecord =
    entries.foldLeft(new RowParquetRecord()) {
      case (record, (key, value)) => record.add(key, value)
    }

  def empty: RowParquetRecord = new RowParquetRecord()

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


  def canEqual(other: Any): Boolean = other.isInstanceOf[RowParquetRecord]

  override def equals(other: Any): Boolean = other match {
    case that: RowParquetRecord =>
      (that canEqual this) && values == that.values
    case _ => false
  }

  override def hashCode(): Int = {
    val state = Seq(values)
    state.map(_.hashCode()).foldLeft(0)((a, b) => 31 * a + b)
  }
}

object ListParquetRecord {

  private val ListFieldName = "list"
  private val ElementFieldName = "element"

  def apply(elements: Value*): ListParquetRecord =
    elements.foldLeft(new ListParquetRecord()) {
      case (record, element) => record.add(ListFieldName, RowParquetRecord(ElementFieldName -> element))
    }

  def empty: ListParquetRecord = new ListParquetRecord()

}

class ListParquetRecord private extends ParquetRecord {
  import ListParquetRecord._

  override type Self = this.type

  private val values = ArrayBuffer.empty[Value]

  override def add(name: String, value: Value): Self = {
    // name should always be equal to ListFieldName
    val element = value.asInstanceOf[RowParquetRecord].getMap.getOrElse(ElementFieldName, NullValue)
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

  def canEqual(other: Any): Boolean = other.isInstanceOf[ListParquetRecord]

  override def equals(other: Any): Boolean = other match {
    case that: ListParquetRecord =>
      (that canEqual this) && values == that.values
    case _ => false
  }

  override def hashCode(): Int = {
    val state = Seq(values)
    state.map(_.hashCode()).foldLeft(0)((a, b) => 31 * a + b)
  }

}

object MapParquetRecord {

  def apply(entries : (Value, Value)*): MapParquetRecord = {
    entries.foldLeft(new MapParquetRecord()) {
      case (record, (key, value)) => record.add("key_value", RowParquetRecord("key" -> key, "value" -> value)) // TODO
    }
  }

  def empty: MapParquetRecord = new MapParquetRecord()

}

class MapParquetRecord private extends ParquetRecord {

  override type Self = this.type

  private val values = scala.collection.mutable.Map.empty[Value, Value]

  override def add(name: String, value: Value): Self = {
    val keyValueRecord = value.asInstanceOf[RowParquetRecord]
    val mapKey = keyValueRecord.getMap("key")
    val mapValue = keyValueRecord.getMap.getOrElse("value", NullValue)
    values.put(mapKey, mapValue)
    this
  }

  def getMap: Map[Value, Value] = values.toMap

  override def toString: String =
    values
      .map { case (key, value) => s"$key=$value"}
      .mkString(getClass.getSimpleName + " (", ",", ")")

  override def write(schema: Type, recordConsumer: RecordConsumer): Unit = ??? // TODO

  def canEqual(other: Any): Boolean = other.isInstanceOf[MapParquetRecord]

  override def equals(other: Any): Boolean = other match {
    case that: MapParquetRecord =>
      (that canEqual this) && values == that.values
    case _ => false
  }

  override def hashCode(): Int = {
    val state = Seq(values)
    state.map(_.hashCode()).foldLeft(0)((a, b) => 31 * a + b)
  }

}
