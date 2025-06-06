package com.github.mjakubowski84.parquet4s

import com.github.mjakubowski84.parquet4s.compat.MapCompat
import org.apache.parquet.io.api.RecordConsumer
import org.apache.parquet.schema.{MessageType, Type}

import scala.annotation.tailrec
import scala.collection.immutable

/** Special type of [[Value]] that represents a record in Parquet file. Mutable and <b>NOT</b> thread-safe. A record is
  * a complex type of data that contains series of other value entries inside.
  */
sealed trait ParquetRecord[+A, This <: ParquetRecord[A, ?]] extends Value with immutable.Iterable[A] {

  /** Creates a new entry in record.
    * @param name
    *   name of the entry
    * @param value
    *   value of the entry
    * @return
    *   a record with an entry added
    */
  protected[parquet4s] def add(name: String, value: Value): This

  override def toString: String

}

object RowParquetRecord {

  protected object Fields {
    def apply(names: Iterable[String]): Fields = new Fields(names.toSet, names.toVector)
  }

  /** Represents the current state of schema of the record.
    * @param names
    *   unique names of fields
    * @param positions
    *   order of fields in the record
    */
  final protected class Fields private (names: Set[String], positions: Vector[String]) {

    def contains(name: String): Boolean = names.contains(name)

    def get(idx: Int): String = positions.apply(idx)

    def size: Int = positions.length

    def set(idx: Int, name: String, removeOldField: Boolean): Fields = {
      val modifiedNames =
        if (removeOldField) names - positions.apply(idx) + name
        else names + name
      new Fields(modifiedNames, positions.updated(idx, name))
    }

    def remove(idx: Int): (String, Fields) = {
      val name  = positions(idx)
      val front = positions.slice(0, idx)
      val tail  = positions.slice(idx + 1, size)
      (name, new Fields(names - name, front ++ tail))
    }

    def remove(name: String): Fields =
      new Fields(names - name, positions.filterNot(_ == name))

    def prepend(name: String): Fields =
      new Fields(names + name, name +: positions)

    def append(name: String): Fields =
      new Fields(names + name, positions :+ name)

    def iterator: Iterator[String] = positions.iterator

    def count(name: String): Int = positions.count(_ == name)

    override def toString: String = positions.mkString("[", ",", "]")

    override def hashCode(): Int = positions.hashCode
  }

  /** @param fields
    *   fields and their values to init the record with
    * @return
    *   A new instance of [[RowParquetRecord]] initialized with given list of fields.
    */
  def apply(fields: Iterable[(String, Value)]): RowParquetRecord =
    fields.foldLeft(new RowParquetRecord(Map.empty, fields = RowParquetRecord.Fields(fields.map(_._1)))) {
      case (record, (fieldName, value)) => record.add(fieldName, value)
    }

  /** @param fields
    *   fields and their values to init the record with
    * @return
    *   A new instance of [[RowParquetRecord]] initialized with given list of fields.
    */
  def apply(fields: (String, Value)*): RowParquetRecord =
    apply(fields)

  /** @param columns
    *   columns and their values to init the record with
    * @return
    *   A new instance of [[RowParquetRecord]] initialized with given list of columns.
    */
  def fromColumns(columns: (ColumnPath, Value)*): RowParquetRecord =
    columns.foldLeft(
      new RowParquetRecord(Map.empty, fields = RowParquetRecord.Fields(columns.map(_._1.elements.head)))
    ) { case (record, (path, value)) =>
      record.updated(path, value)
    }

  /** @param fields
    *   fields to init the record with
    * @return
    *   A new instance of [[RowParquetRecord]] initialized with [[NullValue]] per each field.
    */
  def emptyWithSchema(fields: Iterable[String]): RowParquetRecord =
    new RowParquetRecord(
      fields.map(_ -> NullValue).toMap,
      RowParquetRecord.Fields(fields)
    )

  /** @param fields
    *   fields to init the record with
    * @return
    *   A new instance of [[RowParquetRecord]] initialized with [[NullValue]] per each field.
    */
  def emptyWithSchema(fields: String*): RowParquetRecord =
    emptyWithSchema(fields)

  /** Empty record holding no field and no value.
    */
  val EmptyNoSchema: RowParquetRecord = new RowParquetRecord(Map.empty, RowParquetRecord.Fields(Iterable.empty))

  implicit val genericParquetRecordEncoder: ParquetRecordEncoder[RowParquetRecord] = (record, _, _) => record

  implicit val genericParquetRecordDecoder: ParquetRecordDecoder[RowParquetRecord] = (record, _) => record

  implicit def genericParquetSchemaResolver(implicit message: MessageType): ParquetSchemaResolver[RowParquetRecord] =
    new ParquetSchemaResolver[RowParquetRecord] {
      override def schemaName: Option[String]                = Option(message.getName)
      override def resolveSchema(cursor: Cursor): List[Type] = ParquetSchemaResolver.applyCursor(cursor, message)
    }

}

/** Represents a basic type of [[ParquetRecord]] - an object that contains a list of fields with other values associated
  * with each of them. Used to represent a [[scala.Product]], e.g: case class. <b>Cannot be empty when being saved.</b>
  * Immutable. During reading the record is initialised with list of fields defined Parquet metadata. First, all the
  * values are [[NullValue]]. Subsequently nulls are replaced by the actual value stored in the file. The schema can be
  * also adjusted in order to reflect partition values that are not originally stored in a file but in the directory
  * structure.
  */
final class RowParquetRecord private (
    private val values: Map[String, Value],
    protected val fields: RowParquetRecord.Fields
) extends ParquetRecord[(String, Value), RowParquetRecord]
    with immutable.Seq[(String, Value)] {

  override protected[parquet4s] def add(name: String, value: Value): RowParquetRecord =
    // TODO consider removing this check as it is just safeguarding algorithm itself
    if (fields.contains(name)) {
      new RowParquetRecord(values.updated(name, value), fields)
    } else {
      throw new IllegalArgumentException(s"Field $name does not correspond to records schema: $fields.")
    }

  /** Adds or updates a value at the given path. Creates intermediate records at the path if missing.
    *
    * @param path
    *   [[ColumnPath]]
    * @param value
    *   value to be set at the end of the path
    * @return
    *   this record with the value added or updated
    * @throws scala.IllegalArgumentException
    *   if the path is invalid
    */
  def updated(path: ColumnPath, value: Value): RowParquetRecord =
    path match {
      case ColumnPath(name, ColumnPath.Empty) =>
        this.updated(name, value)
      case ColumnPath(name, tail) =>
        this.get(name) match {
          case Some(NullValue) | None =>
            this.updated(name, RowParquetRecord.fromColumns(tail -> value))
          case Some(existingSubRecord: RowParquetRecord) =>
            this.updated(name, existingSubRecord.updated(tail, value))
          case _ =>
            throw new IllegalArgumentException("Invalid path when setting value of nested record")
        }
      case _ =>
        this
    }

  /** @param fieldName
    *   field/column name
    * @return
    *   [[scala.Some]] value associated with the field name or [[scala.None]] if the field is unknown
    */
  def get(fieldName: String): Option[Value] =
    if (fields.contains(fieldName)) {
      Some(values.getOrElse(fieldName, NullValue))
    } else {
      None
    }

  /** Retrieves value from the record and decodes it. Decodes the value using provided implicit [[ValueDecoder]].
    * @param fieldName
    *   field/column name
    * @param valueCodecConfiguration
    *   codec configuration
    * @param valueDecoder
    *   [[ValueDecoder]]
    * @tparam T
    *   type of read value
    * @return
    *   [[scala.Some]] decoded field value or [[scala.None]] if such field does not exist
    */
  def get[T](fieldName: String, valueCodecConfiguration: ValueCodecConfiguration)(implicit
      valueDecoder: ValueDecoder[T]
  ): Option[T] =
    get(fieldName).map(valueDecoder.decode(_, valueCodecConfiguration))

  /** @param path
    *   [[ColumnPath]]
    * @return
    *   [[scala.Some]] value associated with given path or [[scala.None]] if such field does not exist
    */
  @tailrec
  def get(path: ColumnPath): Option[Value] =
    path match {
      case ColumnPath(fieldName, ColumnPath.Empty) =>
        get(fieldName)
      case ColumnPath(fieldName, tail) =>
        get(fieldName) match {
          case Some(subRecord: RowParquetRecord) => subRecord.get(tail)
          case _                                 => None
        }
      case _ =>
        Some(this)
    }

  override def iterator: Iterator[(String, Value)] =
    fields.iterator.map(name => name -> values(name))

  /** Get the field name and value at the specified index.
    *
    * @param idx
    *   The index
    * @return
    *   The field name and value
    * @throws scala.IndexOutOfBoundsException
    *   if the index is not valid.
    */
  override def apply(idx: Int): (String, Value) =
    (fields.get _).andThen(name => name -> values(name))(idx)

  /** Replaces value at given index with a new value.
    *
    * @param idx
    *   the index of the value to replace.
    * @param newVal
    *   the new value.
    * @return
    *   updated record
    * @throws scala.IndexOutOfBoundsException
    *   if the index is not valid.
    */
  def updated(idx: Int, newVal: Value): RowParquetRecord =
    new RowParquetRecord(values.updated(fields.get(idx), newVal), fields)

  /** Updates existing field or appends a new field to the record if it doesn't exist yet.
    * @param fieldName
    *   the name of the field
    * @param value
    *   the value of the field
    * @return
    *   updated record
    */
  def updated(fieldName: String, value: Value): RowParquetRecord =
    if (fields.contains(fieldName)) {
      new RowParquetRecord(values.updated(fieldName, value), fields)
    } else {
      new RowParquetRecord(values.updated(fieldName, value), fields.append(fieldName))
    }

  /** Updates existing field or appends a new field to the record if it doesn't exist yet. Encodes the provided value
    * using implicit [[ValueEncoder]].
    * @param name
    *   the name of the field
    * @param value
    *   the value of the field
    * @param valueCodecConfiguration
    *   codec configuration
    * @param valueEncoder
    *   [[ValueEncoder]]
    * @tparam T
    *   the type of the value
    * @return
    *   updated record
    */
  def updated[T](name: String, value: T, valueCodecConfiguration: ValueCodecConfiguration)(implicit
      valueEncoder: ValueEncoder[T]
  ): RowParquetRecord =
    updated(name, valueEncoder.encode(value, valueCodecConfiguration))

  /** Updates existing field at given index with a new name and a new value.
    * @param idx
    *   index of the field
    * @param field
    *   a new name of the field
    * @param newVal
    *   a new value of the field
    * @return
    *   record with the field updated
    * @throws scala.IndexOutOfBoundsException
    *   when the index is invalid
    */
  def updated(idx: Int, field: String, newVal: Value): RowParquetRecord = {
    val oldField = fields.get(idx)
    val count    = fields.count(oldField)
    if (count == 1)
      new RowParquetRecord(
        values = MapCompat.remove(values, oldField).updated(field, newVal),
        fields = fields.set(idx, field, removeOldField = true)
      )
    else
      new RowParquetRecord(
        values = values.updated(field, newVal),
        fields = fields.set(idx, field, removeOldField = false)
      )
  }

  /** Removes field at given index.
    * @param idx
    *   index of the field to be removed
    * @return
    *   a tuple of the name of removed field and its value associated with the resulting record
    * @throws scala.IndexOutOfBoundsException
    *   if the index is not valid.
    */
  def removed(idx: Int): ((String, Value), RowParquetRecord) = {
    val (field, newFields) = fields.remove(idx)
    val value              = values(field)
    (field -> value, new RowParquetRecord(MapCompat.remove(values, field), newFields))
  }

  /** Removes field of given name.
    * @param fieldName
    *   name of the field to be removed
    * @return
    *   [[scala.Some]] Value of the removed field or [[scala.None]] if no such a field exists, associated with the
    *   resulting record
    */
  def removed(fieldName: String): (Option[Value], RowParquetRecord) =
    get(fieldName) match {
      case None =>
        (None, this)
      case value =>
        (value, new RowParquetRecord(MapCompat.remove(values, fieldName), fields.remove(fieldName)))
    }

  /** Removes field at given path.
    *
    * @param path
    *   [[ColumnPath]]
    * @return
    *   [[scala.Some]] value of the removed field or [[scala.None]] if path is invalid associated with the resulting
    *   record
    */
  def removed(path: ColumnPath): (Option[Value], RowParquetRecord) =
    path match {
      case ColumnPath(fieldName, ColumnPath.Empty) =>
        removed(fieldName)
      case ColumnPath(fieldName, tail) =>
        get(fieldName) match {
          case Some(subRecord: RowParquetRecord) =>
            val (valueOpt, modifiedSubRecord) = subRecord.removed(tail)
            if (modifiedSubRecord.isEmpty) removed(fieldName).copy(_1 = valueOpt)
            else (valueOpt, this.updated(fieldName, modifiedSubRecord))
          case _ =>
            (None, this)
        }
      case _ =>
        (Some(this), RowParquetRecord.EmptyNoSchema)
    }

  /** @return
    *   The number of columns in this record
    */
  override def length: Int = fields.size

  override def toString: String =
    fields.iterator
      .map(name => s"$name=${values(name)}")
      .mkString("RowParquetRecord(", ",", ")")

  /** Adds a new field to the front of the record.
    * @param name
    *   the name of the field
    * @param value
    *   [[Value]] of the field
    * @return
    *   this record with the field prepended
    */
  def prepended(name: String, value: Value): RowParquetRecord =
    new RowParquetRecord(values.updated(name, value), fields.prepend(name))

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

  /** Decodes this record to given class.
    * @param configuration
    *   decoding configuration
    */
  def as[T: ParquetRecordDecoder](configuration: ValueCodecConfiguration): T =
    ParquetRecordDecoder.decode(this, configuration)

  /** Renames a field in the record
    * @param idx
    *   idx of the field
    * @param newField
    *   field name to change to
    * @return
    *   a record with the name changed
    * @throws scala.IndexOutOfBoundsException
    *   when there's no field at given index
    */
  def rename(idx: Int, newField: String): RowParquetRecord = {
    val oldField = fields.get(idx)
    val count    = fields.count(oldField)
    val value    = values(oldField)
    if (count == 1)
      new RowParquetRecord(
        MapCompat.remove(values, oldField).updated(newField, value),
        fields.set(idx, newField, removeOldField = true)
      )
    else
      new RowParquetRecord(values.updated(newField, value), fields.set(idx, newField, removeOldField = false))
  }

  /** Allows to check if the schema of the record defines a field with given name. That is, there might `NullValue`
    * assigned to the field but as long as the schema contains the field the function returns true.
    * @return
    *   true if a field with given name is defined for this record
    */
  def contains(fieldName: String): Boolean = fields.contains(fieldName)

  /** Appends fields from `other` record to this one. If this record already contains a field with given name then the
    * existing value is replaced with a value from `other` record.
    * @param other
    *   the record to be merged this one
    * @return
    *   a new record with fields coming from this and `other` records
    */
  def merge(other: RowParquetRecord): RowParquetRecord =
    other.foldLeft(this) { case (record, (fieldName, fieldValue)) =>
      record.updated(fieldName, fieldValue)
    }

  override def canEqual(other: Any): Boolean = other.isInstanceOf[RowParquetRecord]

  override def equals(other: Any): Boolean = other match {
    case that: RowParquetRecord =>
      (that canEqual this) && values == that.values
    case _ => false
  }

  override def hashCode(): Int = {
    val state = Seq(values.keys, fields)
    state.map(_.hashCode()).foldLeft(0)((a, b) => 31 * a + b)
  }

}

object ListParquetRecord {
  private[parquet4s] object ElementName {
    val Element      = "element"
    val Array        = "array"
    val ArrayElement = "array_element"
    val Item         = "item"
  }

  private[parquet4s] val ListFieldName = "list"
  private val ElementNames = Set(ElementName.Element, ElementName.Array, ElementName.ArrayElement, ElementName.Item)

  /** @param elements
    *   to init the record with
    * @return
    *   An instance of [[ListParquetRecord]] pre-filled with given elements
    */
  def apply(elements: Value*): ListParquetRecord =
    elements.foldLeft(ListParquetRecord.Empty) { case (record, element) =>
      record.add(ListFieldName, RowParquetRecord.apply(ElementName.Element -> element))
    }

  /** An empty instance of [[ListParquetRecord]]
    */
  val Empty: ListParquetRecord = new ListParquetRecord(Vector.empty)

}

/** A type of [[ParquetRecord]] that represents a record holding a repeated amount of entries of the same type. Can be
  * empty. Immutable.
  */
final class ListParquetRecord private (private val values: Vector[Value])
    extends ParquetRecord[Value, ListParquetRecord]
    with immutable.Seq[Value] {
  import ListParquetRecord.*

  override protected[parquet4s] def add(name: String, value: Value): ListParquetRecord =
    value match {
      case repeated: RowParquetRecord if repeated.length == 1 && ElementNames.contains(repeated.head._1) =>
        appended(repeated.head._2)
      case repeated: RowParquetRecord if repeated.isEmpty =>
        appended(NullValue)
      case _ =>
        appended(value)
    }

  /** Appends value to the list.
    * @param value
    *   the value to append
    * @return
    *   this record with the value appended
    */
  def appended(value: Value): ListParquetRecord = new ListParquetRecord(values :+ value)

  /** Appends value to the list.
    * @param value
    *   the value to append
    * @param valueCodecConfiguration
    *   codec configuration
    * @param valueEncoder
    *   [[ValueEncoder]]
    * @tparam T
    *   type of the value
    * @return
    *   this record with the value appended
    */
  def appended[T](value: T, valueCodecConfiguration: ValueCodecConfiguration)(implicit
      valueEncoder: ValueEncoder[T]
  ): ListParquetRecord =
    this.appended(valueEncoder.encode(value, valueCodecConfiguration))

  /** Gets the value at the specified index.
    *
    * @param idx
    *   The index
    * @return
    *   The value
    * @throws scala.IndexOutOfBoundsException
    *   if the index is not valid
    */
  override def apply(idx: Int): Value = values(idx)

  /** Gets the value at the specified index.
    *
    * @param idx
    *   The index
    * @param valueCodecConfiguration
    *   codec configuration
    * @param valueDecoder
    *   [[ValueDecoder]]
    * @tparam T
    *   type of the value
    * @return
    *   The value
    * @throws scala.IndexOutOfBoundsException
    *   if the index is not valid
    */
  def apply[T](idx: Int, valueCodecConfiguration: ValueCodecConfiguration)(implicit valueDecoder: ValueDecoder[T]): T =
    valueDecoder.decode(this.apply(idx), valueCodecConfiguration)

  /** Replaces value at given index with a new value.
    *
    * @param idx
    *   the index of the element to replace
    * @param newVal
    *   the new value
    * @return
    *   updated record
    * @throws scala.IndexOutOfBoundsException
    *   if the index is not valid.
    */
  def updated(idx: Int, newVal: Value): ListParquetRecord = new ListParquetRecord(values.updated(idx, newVal))

  override def length: Int = values.length

  override def iterator: Iterator[Value] = values.iterator

  override def toString: String = values.mkString("ListParquetRecord(", ",", ")")

  override def write(schema: Type, recordConsumer: RecordConsumer): Unit = {
    recordConsumer.startGroup()

    if (values.nonEmpty) {
      val groupSchema = schema.asGroupType()

      val container   = groupSchema.getFields.get(0).asGroupType()
      val fieldName   = container.getName
      val elementName = container.getFields.get(0).getName

      val listSchema = groupSchema.getType(fieldName).asGroupType()
      val listIndex  = groupSchema.getFieldIndex(fieldName)

      recordConsumer.startField(fieldName, listIndex)

      values.foreach { value =>
        RowParquetRecord.apply(elementName -> value).write(listSchema, recordConsumer)
      }

      recordConsumer.endField(fieldName, listIndex)
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

  private[parquet4s] val MapKeyValueFieldName = "key_value"
  private[parquet4s] val KeyFieldName         = "key"
  private[parquet4s] val ValueFieldName       = "value"

  def apply(entries: (Value, Value)*): MapParquetRecord =
    entries.foldLeft(MapParquetRecord.Empty) { case (record, (key, value)) =>
      record.add(MapKeyValueFieldName, RowParquetRecord.apply(KeyFieldName -> key, ValueFieldName -> value))
    }

  val Empty: MapParquetRecord = new MapParquetRecord(Map.empty)

}

/** A type of [[ParquetRecord]] that represents a map from one entry type to another. Can be empty. A key entry cannot
  * be null, a value entry can. Immutable.
  */
final class MapParquetRecord private[parquet4s] (protected val entries: Map[Value, Value])
    extends ParquetRecord[(Value, Value), MapParquetRecord]
    with immutable.Map[Value, Value]
    with MapCompat {
  import MapParquetRecord.*

  override protected[parquet4s] def add(name: String, value: Value): MapParquetRecord =
    value match {
      case keyValueRecord: RowParquetRecord =>
        (keyValueRecord.get(KeyFieldName), keyValueRecord.get(ValueFieldName)) match {
          case (Some(mapKey), Some(mapValue)) =>
            updated(mapKey, mapValue)
          case _ =>
            // TODO maybe just ignore?
            throw new IllegalArgumentException(s"Missing $KeyFieldName or $ValueFieldName in the $MapKeyValueFieldName")
        }
      case _ =>
        throw new IllegalArgumentException(s"Expected $MapKeyValueFieldName but got $name: $value")
    }

  /** Updates or adds a key-value entry.
    * @param key
    *   the key
    * @param value
    *   the value
    * @return
    *   the modified record
    */
  def updated(key: Value, value: Value): MapParquetRecord = new MapParquetRecord(entries.updated(key, value))

  /** Updates or adds a key-value entry. Encodes the key and the value suing the provided [[ValueEncoder]]s
    * @param key
    *   the key
    * @param value
    *   the value
    * @param valueCodecConfiguration
    *   codec configuration
    * @param kEncoder
    *   [[ValueEncoder]] for the key
    * @param vEncoder
    *   [[ValueEncoder]] for the value
    * @tparam K
    *   type of the key
    * @tparam V
    *   type of the value
    * @return
    *   the modified record
    */
  def updated[K, V](key: K, value: V, valueCodecConfiguration: ValueCodecConfiguration)(implicit
      kEncoder: ValueEncoder[K],
      vEncoder: ValueEncoder[V]
  ): MapParquetRecord =
    updated(kEncoder.encode(key, valueCodecConfiguration), vEncoder.encode(value, valueCodecConfiguration))

  /** Retrieves the value which is associated with the given key.
    *
    * @param key
    *   the key
    * @return
    *   the value associated with the given key, or the result of the map's `default` method, if none exists.
    * @throws scala.NoSuchElementException
    *   if there is no entry for the given key
    */
  override def apply(key: Value): Value = entries(key)

  /** Retrieves the value which is associated with the given key.
    *
    * @param key
    *   the key
    * @param valueCodecConfiguration
    *   configuration used by some of codecs
    * @param kEncoder
    *   key encoder
    * @param vDecoder
    *   value decoder
    * @tparam K
    *   type of the key
    * @tparam V
    *   type of the value
    * @return
    *   retrieved value
    * @throws scala.NoSuchElementException
    *   if there is no entry for the given key
    */
  def apply[K, V](key: K, valueCodecConfiguration: ValueCodecConfiguration)(implicit
      kEncoder: ValueEncoder[K],
      vDecoder: ValueDecoder[V]
  ): V =
    vDecoder.decode(this.apply(kEncoder.encode(key, valueCodecConfiguration)), valueCodecConfiguration)

  /** Optionally returns the value associated with a key.
    *
    * @param key
    *   the key value
    * @return
    *   an option value containing the value associated with `key` in this map, or `None` if none exists.
    */
  override def get(key: Value): Option[Value] = entries.get(key)

  /** Retrieves the value which is associated with the given key.
    *
    * @param key
    *   the key
    * @param valueCodecConfiguration
    *   configuration used by some of codecs
    * @param kEncoder
    *   key codec
    * @param vDecoder
    *   value codec
    * @tparam K
    *   type of the key
    * @tparam V
    *   type of the value
    * @return
    *   retrieved value or None if there is no value associated with the key
    */
  def get[K, V](key: K, valueCodecConfiguration: ValueCodecConfiguration)(implicit
      kEncoder: ValueEncoder[K],
      vDecoder: ValueDecoder[V]
  ): Option[V] =
    this
      .get(kEncoder.encode(key, valueCodecConfiguration))
      .map(v => vDecoder.decode(v, valueCodecConfiguration))

  override def iterator: Iterator[(Value, Value)] = entries.iterator

  override def toString: String =
    entries
      .map { case (key, value) => s"$key=$value" }
      .mkString("MapParquetRecord(", ",", ")")

  override def write(schema: Type, recordConsumer: RecordConsumer): Unit = {
    recordConsumer.startGroup()

    if (entries.nonEmpty) {
      val groupSchema       = schema.asGroupType()
      val mapKeyValueSchema = groupSchema.getType(MapKeyValueFieldName).asGroupType()
      val mapKeyValueIndex  = groupSchema.getFieldIndex(MapKeyValueFieldName)

      recordConsumer.startField(MapKeyValueFieldName, mapKeyValueIndex)

      entries.foreach { case (key, value) =>
        RowParquetRecord.apply(KeyFieldName -> key, ValueFieldName -> value).write(mapKeyValueSchema, recordConsumer)
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
