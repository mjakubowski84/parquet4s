package com.github.mjakubowski84.parquet4s

import org.apache.parquet.io.api.RecordConsumer
import org.apache.parquet.schema.{MessageType, Type}
import scala.collection.JavaConverters._

import scala.collection.mutable

/**
  * Special type of [[Value]] that represents a record in Parquet file.
  * Mutable and <b>NOT</b> thread-safe.
  * A record is a complex type of data that contains series of other value entries inside.
  */
sealed trait ParquetRecord[A] extends Value with Iterable[A] {

  type This

  /**
    * Creates a new entry in record.
    * @param name name of the entry
    * @param value value of the entry
    * @return a record with an entry added
    */
  def add(name: String, value: Value): This

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

  implicit def genericParquetSchemaResolver(implicit message: MessageType): ParquetSchemaResolver[RowParquetRecord] =
    new ParquetSchemaResolver[RowParquetRecord] {
      override def resolveSchema: List[Type] = message.getFields.iterator().asScala.toList
    }

  implicit val genericParquetRecordEncoder: ParquetRecordEncoder[RowParquetRecord] =
    new ParquetRecordEncoder[RowParquetRecord] {
      override def encode(entity: RowParquetRecord, configuration: ValueCodecConfiguration): RowParquetRecord = entity
    }
  
  implicit val genericParquetDecoder: ParquetRecordDecoder[RowParquetRecord] =
    new ParquetRecordDecoder[RowParquetRecord] {
      override def decode(record: RowParquetRecord, configuration: ValueCodecConfiguration): RowParquetRecord = record
    }

}

/**
  * Represents a basic type of [[ParquetRecord]] an object that contains
  * a non-empty list of fields with other values associated with each of them.
  * Cannot be empty while being saved.
  * Mutable and <b>NOT</b> thread-safe.
  */
class RowParquetRecord private extends ParquetRecord[(String, Value)] with mutable.Seq[(String, Value)]{

  override type This = this.type

  private val values = mutable.ArrayBuffer.empty[(String, Value)]

  private var lookupCache: Map[String, Value] = _

  override def add(name: String, value: Value): This = {
    // TODO handle case when this field is already there!
    values.append((name, value))
    lookupCache = null
    this
  }

  /**
    * Encodes the value end appends it to the record.
    */
  def add[T](name: String, value: T, valueCodecConfiguration: ValueCodecConfiguration)(implicit valueCodec: ValueCodec[T]): This =
    add(name, valueCodec.encode(value, valueCodecConfiguration))

  /**
    * @return fields held in record
    */
  @deprecated("1.1", "Use iterator to iterate or other functions to access or modify record elements.")
  def fields: Map[String, Value] = values.toMap

  /**
   *
   * @param fieldName field/column name
   * @return value associated with the field name or [[NullValue]] if no value is found
   */
  def get(fieldName: String): Value = {
    if (lookupCache == null) lookupCache = values.toMap
    lookupCache.getOrElse(fieldName, NullValue)
  }

  /**
    * Retrieves value from the record and decodes it.
    * @param fieldName field/column name
    * @return decoded field value or `null` if such field does not exist
    */
  def get[T](fieldName: String, valueCodecConfiguration: ValueCodecConfiguration)(implicit valueCodec: ValueCodec[T]): T =
    valueCodec.decode(get(fieldName), valueCodecConfiguration)

  override def iterator: Iterator[(String, Value)] = values.iterator

  /** Get the field name and value at the specified index.
    *
    * @param idx The index
    * @return The field name and value
    * @throws IndexOutOfBoundsException if the index is not valid.
    */
  override def apply(idx: Int): (String, Value) = values(idx)


  /** Replaces field name and value at given index with a new field name and value.
    *
    *  @param idx      the index of the element to replace.
    *  @param newEntry     the new field name and value.
    *  @throws IndexOutOfBoundsException if the index is not valid.
    */
  override def update(idx: Int, newEntry: (String, Value)): Unit = {
    values(idx) = newEntry
    lookupCache = null
  }

  /** Replaces value at given index with a new value.
    *
    *  @param idx      the index of the value to replace.
    *  @param newVal   the new value.
    *  @throws IndexOutOfBoundsException if the index is not valid.
    */
  def update(idx: Int, newVal: Value): Unit = values(idx) = values(idx).copy(_2 = newVal)

  /**
    *
    * @return The number of columns in this record
    */
  def length: Int = values.length

  override def toString: String =
    values
      .map { case (key, value) => s"$key=$value"}
      .mkString(getClass.getSimpleName + " (", ",", ")")

  /**
    * Adds a new field to the front of the record.
    */
  def prepend(name: String, value: Value): This = {
    values.prepend((name, value))
    lookupCache = null
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
  * Mutable and <b>NOT</b> thread-safe.
  */
class ListParquetRecord private extends ParquetRecord[Value] with mutable.Seq[Value]{
  import ListParquetRecord._

  override type This = this.type

  private val values = mutable.ArrayBuffer.empty[Value]

  override def add(name: String, value: Value): This =
    value match {
      case repeated: RowParquetRecord if repeated.length==1 && repeated.head._1 == ElementFieldName =>
        add(repeated.head._2)
      case repeated: RowParquetRecord if repeated.isEmpty =>
        add(NullValue)
      case _ =>
        add(value)
    }

  /**
    * Appends value to the list.
    */
  def add(value: Value): This = {
    values.append(value)
    this
  }

  /**
    * Appends value to the list.
    */
  def add[T](value: T, valueCodecConfiguration: ValueCodecConfiguration)(implicit valueCodec: ValueCodec[T]): This =
    this.add(valueCodec.encode(value, valueCodecConfiguration))

  /**
    * @return collection of elements held in record
    */
  @deprecated("1.1", "Use iterator to iterate or other functions to access or modify record elements.")
  def elements: List[Value] = values.toList

  /** Get the value at the specified index.
    *
    * @param idx The index
    * @return The value
    * @throws IndexOutOfBoundsException if the index is not valid.
    */
  def apply(idx: Int): Value = values(idx)

  def apply[T](idx: Int, valueCodecConfiguration: ValueCodecConfiguration)(implicit codec: ValueCodec[T]): T =
    codec.decode(this.apply(idx), valueCodecConfiguration)

  /** Replaces value at given index with a new value.
    *
    *  @param idx      the index of the element to replace.
    *  @param newVal     the new value.
    *  @throws IndexOutOfBoundsException if the index is not valid.
    */
  def update(idx: Int, newVal: Value): Unit = values(idx) = newVal

  def length: Int = values.length

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
  * Mutable and <b>NOT</b> thread-safe.
  */
class MapParquetRecord private extends ParquetRecord[(Value, Value)]
  with mutable.Map[Value, Value]
  with ShrinkableCompat {
  import MapParquetRecord._

  override type This = this.type

  protected val entries = mutable.Map.empty[Value, Value]

  override def add(name: String, value: Value): This = {
    val keyValueRecord = value.asInstanceOf[RowParquetRecord]
    val mapKey = keyValueRecord.get(KeyFieldName)
    val mapValue = keyValueRecord.get(ValueFieldName)
    add(mapKey, mapValue)
  }

  /**
    * The same as [[update]] but returns updated record.
    */
  def add(key: Value, value: Value): This = {
    entries.put(key, value)
    this
  }

  /**
    * The same as [[update]] but returns updated record.
    */
  def add[K, V](key: K, newVal: V, valueCodecConfiguration: ValueCodecConfiguration)
                  (implicit kCodec: ValueCodec[K], vCodec: ValueCodec[V]): This =
    this.add(kCodec.encode(key, valueCodecConfiguration), vCodec.encode(newVal, valueCodecConfiguration))

  /**
    * @return map of values held by record
    */
  @deprecated("1.1", "Use iterator to iterate or other functions to access or modify record elements.")
  def getMap: Map[Value, Value] = entries.toMap

  /** Retrieves the value which is associated with the given key.
    *
    *  @param  key the key
    *  @return     the value associated with the given key, or the result of the
    *              map's `default` method, if none exists.
    *  @throws NoSuchElementException if there is no entry for the given key
    */
  override def apply(key: Value): Value = entries(key)

  /** Retrieves the value which is associated with the given key.
    *
    * @param key the key
    * @param valueCodecConfiguration configuration used by some of codecs
    * @param kCodec key codec
    * @param vCodec value codec
    * @tparam K type of the key
    * @tparam V type of the value
    * @return retrieved value
    * @throws NoSuchElementException if there is no entry for the given key
    */
  def apply[K, V](key: K, valueCodecConfiguration: ValueCodecConfiguration)
                 (implicit kCodec: ValueCodec[K], vCodec: ValueCodec[V]): V =
    vCodec.decode(this.apply(kCodec.encode(key, valueCodecConfiguration)), valueCodecConfiguration)


  /** Optionally returns the value associated with a key.
    *
    *  @param  key    the key value
    *  @return an option value containing the value associated with `key` in this map,
    *          or `None` if none exists.
    */
  override def get(key: Value): Option[Value] = entries.get(key)

  /** Retrieves the value which is associated with the given key.
    *
    * @param key the key
    * @param valueCodecConfiguration configuration used by some of codecs
    * @param kCodec key codec
    * @param vCodec value codec
    * @tparam K type of the key
    * @tparam V type of the value
    * @return retrieved value or None if there is no value associated with the key
    */
  def get[K, V](key: K, valueCodecConfiguration: ValueCodecConfiguration)
               (implicit kCodec: ValueCodec[K], vCodec: ValueCodec[V]): Option[V] =
    this
      .get(kCodec.encode(key, valueCodecConfiguration))
      .map(v => vCodec.decode(v, valueCodecConfiguration))

  /** Adds a new key/value pair to this map.
    *  If the map already contains a
    *  mapping for the key, it will be overridden by the new value.
    *
    *  @param key    The key to update
    *  @param newVal  The new value
    */
  override def update(key: Value, newVal: Value): Unit = entries(key) = newVal

  /**
    * Updates map with a value for a given key
    * @param key the key to update
    * @param newVal the new value
    * @param valueCodecConfiguration configuration used by some codecs
    * @param kCodec key codec
    * @param vCodec value codec
    * @tparam K type of the kye
    * @tparam V type of the value
    */
  def update[K, V](key: K, newVal: V, valueCodecConfiguration: ValueCodecConfiguration)
                  (implicit kCodec: ValueCodec[K], vCodec: ValueCodec[V]): Unit =
    this.update(kCodec.encode(key, valueCodecConfiguration), vCodec.encode(newVal, valueCodecConfiguration))

  override def keys: Iterable[Value] = entries.keys

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
