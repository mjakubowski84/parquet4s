package com.github.mjakubowski84.parquet4s

import com.github.mjakubowski84.parquet4s.ListParquetRecord.fromSeq
import org.apache.parquet.io.api.RecordConsumer
import org.apache.parquet.schema.Type

import scala.collection.{IterableFactoryDefaults, IterableOps, MapFactory, SpecificIterableFactory, StrictOptimizedIterableOps, StrictOptimizedSeqOps, mutable}
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

object RowParquetRecord extends SpecificIterableFactory[(String, Value), RowParquetRecord]{

  /**
    * @param fields fields to init the record with
    * @return A new instance of [[RowParquetRecord]] initialized with given list of fields.
    */
  override def apply(fields : (String, Value)*): RowParquetRecord = fromSeq(fields)

  def fromSeq(fields: Seq[(String, Value)]): RowParquetRecord =
    fields.foldLeft(RowParquetRecord.empty) {
      case (record, (key, value)) => record.add(key, value)
    }

  /**
    * @return A new empty instance of [[RowParquetRecord]]
    */
  def empty: RowParquetRecord = new RowParquetRecord()

  override def newBuilder: mutable.Builder[(String, Value), RowParquetRecord] =
    Seq.newBuilder[(String, Value)].mapResult(fromSeq)

  override def fromSpecific(it: IterableOnce[(String, Value)]): RowParquetRecord = it match {
    case s: Seq[(String, Value)] => fromSeq(s)
    case _ => fromSeq(it.iterator.toSeq)
  }
}

/**
  * Represents a basic type of [[ParquetRecord]] an object that contains
  * a non-empty list of fields with other values associated with each of them.
  * Cannot be empty while being saved.
  */
class RowParquetRecord private extends ParquetRecord[(String, Value)]
                               with mutable.IndexedBuffer[(String, Value)]
                               with mutable.IndexedSeqOps[(String, Value), mutable.IndexedBuffer, RowParquetRecord]
                               with StrictOptimizedSeqOps[(String, Value), mutable.IndexedBuffer, RowParquetRecord] {

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


  override def prepend(elem: (String, Value)): RowParquetRecord.this.type = {values.prepend(elem); this}
  override def insert(idx: Int, elem: (String, Value)): Unit = values.insert(idx, elem)
  override def insertAll(idx: Int, elems: IterableOnce[(String, Value)]): Unit = values.insertAll(idx, elems)
  override def remove(idx: Int): (String, Value) = values.remove(idx)
  override def remove(idx: Int, count: Int): Unit = values.remove(idx, count)
  override def addOne(elem: (String, Value)): RowParquetRecord.this.type = {values.addOne(elem); this}
  override def clear(): Unit = values.clear()

  override def fromSpecific(coll: IterableOnce[(String, Value)]): RowParquetRecord = RowParquetRecord.fromSpecific(coll)
  override protected def newSpecificBuilder: mutable.Builder[(String, Value), RowParquetRecord] = RowParquetRecord.newBuilder
  override def empty: RowParquetRecord = RowParquetRecord.empty


  // Overloading of `appended`, `prepended`, `appendedAll`, `prependedAll`,
  // `map`, `flatMap` and `concat` to return a `ListParquetRecord` when possible
  def concat(suffix: IterableOnce[(String, Value)]): RowParquetRecord = strictOptimizedConcat(suffix, newSpecificBuilder)
  @inline final def ++ (suffix: IterableOnce[(String, Value)]): RowParquetRecord = concat(suffix)
  def appended(fld: (String, Value)): RowParquetRecord = (newSpecificBuilder ++= this += fld).result()
  def appendedAll(suffix: Iterable[(String, Value)]): RowParquetRecord = strictOptimizedConcat(suffix, newSpecificBuilder)
  def prepended(fld: (String, Value)): RowParquetRecord = (newSpecificBuilder += fld ++= this).result()
  def prependedAll(prefix: Iterable[(String, Value)]): RowParquetRecord = (newSpecificBuilder ++= prefix ++= this).result()
  def map(f: (String, Value) => (String, Value)): RowParquetRecord =
    strictOptimizedMap[(String, Value), RowParquetRecord](newSpecificBuilder, f.tupled)
  def flatMap(f: (String, Value) => IterableOnce[(String, Value)]): RowParquetRecord =
    strictOptimizedFlatMap[(String, Value), RowParquetRecord](newSpecificBuilder, f.tupled)



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

object ListParquetRecord extends SpecificIterableFactory[Value, ListParquetRecord] {


  private val ListFieldName = "list"
  private val ElementFieldName = "element"

  /**
    * @param elements to init the record with
    * @return An instance of [[ListParquetRecord]] pre-filled with given elements
    */
  override def apply(elements: Value*): ListParquetRecord = fromSeq(elements)

  def fromSeq(elements: Seq[Value]) =
    elements.foldLeft(ListParquetRecord.empty) {
      case (record, element) => record.add(ListFieldName, RowParquetRecord(ElementFieldName -> element))
    }

  /**
    * @return An empty instance of [[ListParquetRecord]]
    */
  def empty: ListParquetRecord = new ListParquetRecord()

  override def newBuilder: mutable.Builder[Value, ListParquetRecord] =  Seq.newBuilder[Value].mapResult(fromSeq)

  override def fromSpecific(it: IterableOnce[Value]): ListParquetRecord = it match {
    case s: Seq[Value] => fromSeq(s)
    case _ => fromSeq(it.iterator.toSeq)
  }
}

/**
  * A type of [[ParquetRecord]] that represents a record holding a repeated amount of entries
  * of the same type. Can be empty.
  */
class ListParquetRecord private extends ParquetRecord[Value]
                                with mutable.IndexedBuffer[Value]
                                with mutable.IndexedSeqOps[Value, mutable.IndexedBuffer, ListParquetRecord]
                                with StrictOptimizedSeqOps[Value, mutable.IndexedBuffer, ListParquetRecord] {
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


  override def prepend(elem: Value): ListParquetRecord.this.type = {values.prepend(elem); this}
  override def insert(idx: Int, elem: Value): Unit = values.insert(idx, elem)
  override def insertAll(idx: Int, elems: IterableOnce[Value]): Unit = values.insertAll(idx, elems)
  override def remove(idx: Int): Value = values.remove(idx)
  override def remove(idx: Int, count: Int): Unit = values.remove(idx, count)
  override def addOne(elem: Value): ListParquetRecord.this.type = {values.addOne(elem); this}
  override def clear(): Unit = values.clear()

  override def fromSpecific(coll: IterableOnce[Value]): ListParquetRecord = ListParquetRecord.fromSpecific(coll)
  override protected def newSpecificBuilder: mutable.Builder[Value, ListParquetRecord] = ListParquetRecord.newBuilder
  override def empty: ListParquetRecord = ListParquetRecord.empty


  // Overloading of `appended`, `prepended`, `appendedAll`, `prependedAll`,
  // `map`, `flatMap` and `concat` to return a `ListParquetRecord` when possible
  def concat(suffix: IterableOnce[Value]): ListParquetRecord = strictOptimizedConcat(suffix, newSpecificBuilder)
  @inline final def ++ (suffix: IterableOnce[Value]): ListParquetRecord = concat(suffix)
  def appended(base: Value): ListParquetRecord = (newSpecificBuilder ++= this += base).result()
  def appendedAll(suffix: Iterable[Value]): ListParquetRecord = strictOptimizedConcat(suffix, newSpecificBuilder)
  def prepended(base: Value): ListParquetRecord = (newSpecificBuilder += base ++= this).result()
  def prependedAll(prefix: Iterable[Value]): ListParquetRecord = (newSpecificBuilder ++= prefix ++= this).result()
  def map(f: Value => Value): ListParquetRecord = strictOptimizedMap(newSpecificBuilder, f)
  def flatMap(f: Value => IterableOnce[Value]): ListParquetRecord = strictOptimizedFlatMap(newSpecificBuilder, f)

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

object MapParquetRecord extends SpecificIterableFactory[(Value, Value), MapParquetRecord] {

  private val MapKeyValueFieldName = "map"
  private val KeyFieldName = "key"
  private val ValueFieldName = "value"

  override def apply(entries : (Value, Value)*): MapParquetRecord = fromSeq(entries)

  def fromSeq(entries: Seq[(Value, Value)]): MapParquetRecord =
    entries.foldLeft(MapParquetRecord.empty) {
      case (record, (key, value)) => record.add(MapKeyValueFieldName, RowParquetRecord(KeyFieldName -> key, ValueFieldName -> value))
    }

  def fromMap(map: Map[Value, Value]): MapParquetRecord = fromSeq(map.toSeq)

  def empty: MapParquetRecord = new MapParquetRecord()

  override def newBuilder: mutable.Builder[(Value, Value), MapParquetRecord] = Seq.newBuilder[(Value, Value)].mapResult(fromSeq)

  override def fromSpecific(it: IterableOnce[(Value, Value)]): MapParquetRecord = it match {
    case s: Seq[(Value, Value)] => fromSeq(s)
    case _ => fromSeq(it.iterator.toSeq)
  }
}

/**
  * A type of [[ParquetRecord]] that represents a map from one entry type to another. Can be empty.
  * A key entry cannot be null, a value entry can.
  */
class MapParquetRecord private extends ParquetRecord[(Value, Value)]
                               with mutable.Map[Value, Value]
                               with mutable.MapOps[Value, Value, mutable.Map, MapParquetRecord]
                               with StrictOptimizedIterableOps[(Value, Value), mutable.Iterable, MapParquetRecord]{
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


  protected override def fromSpecific(coll: IterableOnce[(Value, Value)]): MapParquetRecord = MapParquetRecord.fromSpecific(coll)
  protected override def newSpecificBuilder: mutable.Builder[(Value, Value), MapParquetRecord] = MapParquetRecord.newBuilder
  override def empty: MapParquetRecord = MapParquetRecord.empty

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
