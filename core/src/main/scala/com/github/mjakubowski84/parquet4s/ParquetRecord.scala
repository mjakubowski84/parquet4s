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
    entries.foldLeft(RowParquetRecord.empty) {
      case (record, (key, value)) => record.add(key, value)
    }

  def empty: RowParquetRecord = new RowParquetRecord()

}

class RowParquetRecord private extends ParquetRecord {

  override type Self = this.type

  private val values = ArrayBuffer.empty[(String, Value)]

  override def add(name: String, value: Value): Self = {
    // TODO handle case when this field is already there!
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
    elements.foldLeft(ListParquetRecord.empty) {
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
    add(value.asInstanceOf[RowParquetRecord].getMap.getOrElse(ElementFieldName, NullValue))
  }

  def add(value: Value): Self = {
    values.append(value)
    this
  }

  def elements: List[Value] = values.toList // TODO let's use vector, check if ArrayBuffer is a good choice

  override def toString: String = values.mkString(getClass.getSimpleName + " (", ",", ")")

  override def write(schema: Type, recordConsumer: RecordConsumer): Unit = {
    recordConsumer.startGroup()

    if (!isEmpty) {
      val groupSchema = schema.asGroupType()
      val listSchema = groupSchema.getType(ListFieldName).asGroupType()
      val listIndex = groupSchema.getFieldIndex(ListFieldName)

      recordConsumer.startField(ListFieldName, listIndex)

      values.foreach { value =>
        RowParquetRecord(ElementFieldName -> value).write(listSchema, recordConsumer)
      }

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

  private val MapKeyValueFieldName = "map"
  private val KeyFieldName = "key"
  private val ValueFieldName = "value"

  def apply(entries : (Value, Value)*): MapParquetRecord = {
    entries.foldLeft(MapParquetRecord.empty) {
      case (record, (key, value)) => record.add(MapKeyValueFieldName, RowParquetRecord(KeyFieldName -> key, ValueFieldName -> value))
    }
  }

  def empty: MapParquetRecord = new MapParquetRecord()

}

class MapParquetRecord private extends ParquetRecord {
  import MapParquetRecord._

  override type Self = this.type

  private val values = scala.collection.mutable.Map.empty[Value, Value]

  override def add(name: String, value: Value): Self = {
    val keyValueRecord = value.asInstanceOf[RowParquetRecord]
    val mapKey = keyValueRecord.getMap("key")
    val mapValue = keyValueRecord.getMap.getOrElse("value", NullValue)
    add(mapKey, mapValue)
  }

  def add(key: Value, value: Value): Self = {
    values.put(key, value)
    this
  }

  def getMap: Map[Value, Value] = values.toMap

  override def toString: String =
    values
      .map { case (key, value) => s"$key=$value"}
      .mkString(getClass.getSimpleName + " (", ",", ")")

  override def write(schema: Type, recordConsumer: RecordConsumer): Unit = {
    recordConsumer.startGroup()

    if (values.nonEmpty) {
      val groupSchema = schema.asGroupType()
      val mapKeyValueSchema = groupSchema.getType(MapKeyValueFieldName).asGroupType()
      val mapKeyValueIndex = groupSchema.getFieldIndex(MapKeyValueFieldName)

      recordConsumer.startField(MapKeyValueFieldName, mapKeyValueIndex)

      values.foreach { case (key, value) =>
        RowParquetRecord(KeyFieldName -> key, ValueFieldName -> value).write(mapKeyValueSchema, recordConsumer)
      }

      recordConsumer.endField(MapKeyValueFieldName, mapKeyValueIndex)
    }

    recordConsumer.endGroup()
  }

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
