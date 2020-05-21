package com.github.mjakubowski84.parquet4s

import org.apache.parquet.io.api.RecordConsumer
import org.apache.parquet.schema.Type

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

/**
  * Special type of [[Value]] that represents a record in Parquet file.
  * A record is a complex type of data that contains series of other value entries inside.
  */
sealed trait ParquetRecord[A] extends Value with Iterable[A] {

  type Self

  /**
    * Creates a new entry in record.
    * @param name name of the entry
    * @param value value of the entry
    * @return a record with an entry added
    */
  def add(name: String, value: Value): Self

  override def toString: String

}

object RowParquetRecord {

  /**
    * @param fields fields to init the record with
    * @return A new instance of [[RowParquetRecord]] initialized with given list of fields.
    */
  def apply(fields : (String, Value)*): RowParquetRecord =
    fields.foldLeft(RowParquetRecord.empty) {
      case (record, (key, value)) => record.add(key, value)
    }

  /**
    * @return A new empty instance of [[RowParquetRecord]]
    */
  def empty: RowParquetRecord = new RowParquetRecord()

}

/**
  * Represents a basic type of [[ParquetRecord]] an object that contains
  * a non-empty list of fields with other values associated with each of them.
  * Cannot be empty while being saved.
  */
class RowParquetRecord private extends ParquetRecord[(String, Value)] with mutable.Seq[(String, Value)]{

  override type Self = this.type

  private val values = ArrayBuffer.empty[(String, Value)]

  override def add(name: String, value: Value): Self = {
    // TODO handle case when this field is already there!
    values.append((name, value))
    this
  }

  /**
    * @return fields held in record
    */
  def fields: Map[String, Value] = values.toMap


  override def iterator: Iterator[(String, Value)] = values.iterator

  /** Get the field name and value at the specified index.
    *
    * @param idx The index
    * @return The field name and value
    *  @throws   IndexOutOfBoundsException if the index is not valid.
    */
  override def apply(idx: Int) = values(idx)


  /** Replaces field name and value at given index with a new field name and value.
    *
    *  @param idx      the index of the element to replace.
    *  @param newEntry     the new field name and value.
    *  @throws   IndexOutOfBoundsException if the index is not valid.
    */
  override def update(idx: Int, newEntry: (String, Value)): Unit = values(idx) = newEntry

  /** Replaces value at given index with a new value.
    *
    *  @param idx      the index of the value to replace.
    *  @param newVal     the new value.
    *  @throws   IndexOutOfBoundsException if the index is not valid.
    */
  def update(idx: Int, newVal: Value) = values(idx) = values(idx).copy(_2 = newVal)

  /**
    *
    * @return The number of columns in this record
    */
  def length = values.length

  override def toString: String =
    values
      .map { case (key, value) => s"$key=$value"}
      .mkString(getClass.getSimpleName + " (", ",", ")")

  def prepend(name: String, value: Value): RowParquetRecord = {
    values.prepend((name, value))
    this
  }

  override def write(schema: Type, recordConsumer: RecordConsumer): Unit = {
    val groupSchema = schema.asGroupType()
    recordConsumer.startGroup()
    values.foreach {
      case (_, NullValue) =>
        // we do not write nulls
      case (name, value) =>
        val index = groupSchema.getFieldIndex(name)
        recordConsumer.startField(name, index)
        value.write(groupSchema.getType(name), recordConsumer)
        recordConsumer.endField(name, index)
    }
    recordConsumer.endGroup()
  }


  override def canEqual(other: Any): Boolean = other.isInstanceOf[RowParquetRecord]

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

  /**
    * @param elements to init the record with
    * @return An instance of [[ListParquetRecord]] pre-filled with given elements
    */
  def apply(elements: Value*): ListParquetRecord =
    elements.foldLeft(ListParquetRecord.empty) {
      case (record, element) => record.add(ListFieldName, RowParquetRecord(ElementFieldName -> element))
    }

  /**
    * @return An empty instance of [[ListParquetRecord]]
    */
  def empty: ListParquetRecord = new ListParquetRecord()

}

/**
  * A type of [[ParquetRecord]] that represents a record holding a repeated amount of entries
  * of the same type. Can be empty.
  */
class ListParquetRecord private extends ParquetRecord[Value] with mutable.Seq[Value]{
  import ListParquetRecord._

  override type Self = this.type

  private val values = ArrayBuffer.empty[Value]

  override def add(name: String, value: Value): Self = {
    // name should always be equal to ListFieldName
    add(value.asInstanceOf[RowParquetRecord].fields.getOrElse(ElementFieldName, NullValue))
  }

  def add(value: Value): Self = {
    values.append(value)
    this
  }

  /**
    * @return collection of elements held in record
    */
  def elements: List[Value] = values.toList // TODO let's use vector, check if ArrayBuffer is a good choice

  /** Get the value at the specified index.
    *
    * @param idx The index
    * @return The value
    *  @throws   IndexOutOfBoundsException if the index is not valid.
    */
  def apply(idx: Int) = values(idx)

  /** Replaces value at given index with a new value.
    *
    *  @param idx      the index of the element to replace.
    *  @param newVal     the new value.
    *  @throws   IndexOutOfBoundsException if the index is not valid.
    */
  def update(idx: Int, newVal: Value) = values(idx) = newVal
  def length = values.length


  override def iterator: Iterator[Value] = values.iterator

  override def toString: String = values.mkString(getClass.getSimpleName + " (", ",", ")")

  override def write(schema: Type, recordConsumer: RecordConsumer): Unit = {
    recordConsumer.startGroup()

    if (values.nonEmpty) {
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

  override def canEqual(other: Any): Boolean = other.isInstanceOf[ListParquetRecord]

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

/**
  * A type of [[ParquetRecord]] that represents a map from one entry type to another. Can be empty.
  * A key entry cannot be null, a value entry can.
  */
class MapParquetRecord private extends ParquetRecord[(Value, Value)] with mutable.Map[Value, Value] {
  import MapParquetRecord._

  override type Self = this.type

  private val entries = scala.collection.mutable.Map.empty[Value, Value]

  override def add(name: String, value: Value): Self = {
    val keyValueRecord = value.asInstanceOf[RowParquetRecord]
    val mapKey = keyValueRecord.fields("key")
    val mapValue = keyValueRecord.fields.getOrElse("value", NullValue)
    add(mapKey, mapValue)
  }

  def add(key: Value, value: Value): Self = {
    entries.put(key, value)
    this
  }

  /**
    * @return map of values held by record
    */
  def getMap: Map[Value, Value] = entries.toMap

  /** Retrieves the value which is associated with the given key.
    * If there is no entry for the given key,throws a
    *  `NoSuchElementException`.
    *
    *  @param  key the key
    *  @return     the value associated with the given key, or the result of the
    *              map's `default` method, if none exists.
    */
  override def apply(key: Value): Value = entries(key)


  /** Optionally returns the value associated with a key.
    *
    *  @param  key    the key value
    *  @return an option value containing the value associated with `key` in this map,
    *          or `None` if none exists.
    */
  override def get(key: Value): Option[Value] = entries.get(key)

  /** Adds a new key/value pair to this map.
    *  If the map already contains a
    *  mapping for the key, it will be overridden by the new value.
    *
    *  @param key    The key to update
    *  @param newVal  The new value
    */
  override def update(key: Value, newVal: Value) = entries(key) = newVal

  override def keys = entries.keys

  /** Removes a single entry from this map.
    *
    *  @param key  the key of the entry to remove.
    *  @return the MapParquetRecord itself
    */
  override def subtractOne(key: Value) = {
    entries.subtractOne(key)
    this
  }


  /** ${Add}s a single element to this map.
    *
    *  @param elem  the element to $add.
    *  @return the MapParquetRecord itself
    */
  override def addOne(elem: (Value, Value)) = {
    entries.addOne(elem)
    this
  }

  override def iterator: Iterator[(Value, Value)] = entries.iterator

  override def toString: String =
    entries
      .map { case (key, value) => s"$key=$value"}
      .mkString(getClass.getSimpleName + " (", ",", ")")

  override def write(schema: Type, recordConsumer: RecordConsumer): Unit = {
    recordConsumer.startGroup()

    if (entries.nonEmpty) {
      val groupSchema = schema.asGroupType()
      val mapKeyValueSchema = groupSchema.getType(MapKeyValueFieldName).asGroupType()
      val mapKeyValueIndex = groupSchema.getFieldIndex(MapKeyValueFieldName)

      recordConsumer.startField(MapKeyValueFieldName, mapKeyValueIndex)

      entries.foreach { case (key, value) =>
        RowParquetRecord(KeyFieldName -> key, ValueFieldName -> value).write(mapKeyValueSchema, recordConsumer)
      }

      recordConsumer.endField(MapKeyValueFieldName, mapKeyValueIndex)
    }

    recordConsumer.endGroup()
  }

  override def canEqual(other: Any): Boolean = other.isInstanceOf[MapParquetRecord]

  override def equals(other: Any): Boolean = other match {
    case that: MapParquetRecord =>
      (that canEqual this) && entries == that.entries
    case _ => false
  }

  override def hashCode(): Int = {
    val state = Seq(entries)
    state.map(_.hashCode()).foldLeft(0)((a, b) => 31 * a + b)
  }

}
