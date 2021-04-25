package com.github.mjakubowski84.parquet4s

import com.github.mjakubowski84.parquet4s.ParquetSchemaResolver.TypedSchemaDef
import org.apache.parquet.schema.LogicalTypeAnnotation.{DateLogicalTypeAnnotation, DecimalLogicalTypeAnnotation, IntLogicalTypeAnnotation, StringLogicalTypeAnnotation}
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.{BINARY, BOOLEAN, DOUBLE, FIXED_LEN_BYTE_ARRAY, FLOAT, INT32, INT64, INT96}
import org.apache.parquet.schema.Type.Repetition
import org.apache.parquet.schema._
import shapeless._
import shapeless.labelled._

import scala.language.higherKinds
import scala.reflect.ClassTag

/**
  * Type class that allows to build schema of Parquet file out from regular Scala type, typically case class.
  * @tparam T scala type that represents schema of Parquet data.
  */
trait ParquetSchemaResolver[T] {

  /**
    * @return list of [[org.apache.parquet.schema.Type]] for each product element that <i>T</i> contains.
    */
  def resolveSchema: List[Type]

  /**
    * @return a name to be given to schema
    */
  def schemaName: Option[String] = None

}

object ParquetSchemaResolver
  extends SchemaDefs {

  trait Tag[V]
  type TypedSchemaDef[V] = SchemaDef with Tag[V]

  /**
    * Builds full Parquet file schema ([[org.apache.parquet.schema.MessageType]]) from <i>T</i>.
    */
  def resolveSchema[T](implicit g: ParquetSchemaResolver[T]): MessageType =
    Message(g.schemaName, g.resolveSchema:_*)

  implicit val hnil: ParquetSchemaResolver[HNil] = new ParquetSchemaResolver[HNil] {
    override def resolveSchema: List[Type] = List.empty
  }

  implicit def hcons[K <: Symbol, V, T <: HList](implicit
                                                 witness: Witness.Aux[K],
                                                 schemaDef: TypedSchemaDef[V],
                                                 rest: ParquetSchemaResolver[T]
                                                ): ParquetSchemaResolver[FieldType[K, V] :: T] =
    new ParquetSchemaResolver[FieldType[K, V] :: T] {
      override def resolveSchema: List[Type] = schemaDef(witness.value.name) +: rest.resolveSchema
    }

  implicit def generic[T, G](implicit
                             lg: LabelledGeneric.Aux[T, G],
                             rest: Lazy[ParquetSchemaResolver[G]],
                             classTag: ClassTag[T]
                            ): ParquetSchemaResolver[T] = new ParquetSchemaResolver[T] {
    override def resolveSchema: List[Type] = rest.value.resolveSchema
    override def schemaName: Option[String] = Option(classTag.runtimeClass.getCanonicalName)
  }
}

object Message {

  val DefaultName = "parquet4s_schema"

  def apply(name: Option[String], fields: Type*): MessageType =
    Types.buildMessage().addFields(fields:_*).named(name.getOrElse(DefaultName))

}

trait SchemaDef {

  type Self <: SchemaDef

  def apply(name: String): Type

  def withRequired(required: Boolean): Self

  def typed[V]: ParquetSchemaResolver.TypedSchemaDef[V] = ParquetSchemaResolver.typedSchemaDef[V](this)

}

object SchemaDef {
  def primitive(primitiveType: PrimitiveType.PrimitiveTypeName,
                logicalTypeAnnotation: Option[LogicalTypeAnnotation] = None,
                required: Boolean = true,
                length: Option[Int] = None): SchemaDef =
    PrimitiveSchemaDef(primitiveType, logicalTypeAnnotation, required, length)

  def group(fields: Type*): SchemaDef =
    GroupSchemaDef(fields, required = false)

  def list(elementSchemaDef: SchemaDef): SchemaDef =
    ListSchemaDef(elementSchemaDef(ListSchemaDef.ElementName), required = false)

  def map(keySchemaDef: SchemaDef, valueSchemaDef: SchemaDef): SchemaDef = MapSchemaDef(
    keySchemaDef(MapSchemaDef.KeyName), valueSchemaDef(MapSchemaDef.ValueName), required = false
  )
}

object LogicalTypes {
  val Int64Type: IntLogicalTypeAnnotation = LogicalTypeAnnotation.intType(64, true)
  val Int32Type: IntLogicalTypeAnnotation = LogicalTypeAnnotation.intType(32, true)
  val Int16Type: IntLogicalTypeAnnotation = LogicalTypeAnnotation.intType(16, true)
  val Int8Type: IntLogicalTypeAnnotation = LogicalTypeAnnotation.intType(8, true)
  val DecimalType: DecimalLogicalTypeAnnotation = LogicalTypeAnnotation.decimalType(Decimals.Scale, Decimals.Precision)
  val StringType: StringLogicalTypeAnnotation = LogicalTypeAnnotation.stringType()
  val DateType: DateLogicalTypeAnnotation = LogicalTypeAnnotation.dateType()
}

object PrimitiveSchemaDef {
  @deprecated("Use SchemaDef.primitive", since = "1.9.0")
  def apply(
             primitiveType: PrimitiveType.PrimitiveTypeName,
             required: Boolean = true,
             originalType: Option[OriginalType] = None,
             precision: Option[Int] = None,
             scale: Option[Int] = None,
             length: Option[Int] = None
           ): PrimitiveSchemaDef = {
    val decimalMetaData = (precision, scale) match {
      case (Some(p), Some(s)) => new DecimalMetadata(p, s)
      case _ => null
    }
    PrimitiveSchemaDef(
      primitiveType = primitiveType,
      required = required,
      logicalTypeAnnotation = originalType.map(ot => LogicalTypeAnnotation.fromOriginalType(ot, decimalMetaData)),
      length = length
    )
  }
}

@deprecated("Use SchemaDef.primitive", since = "1.9.0")
case class PrimitiveSchemaDef private(
                                       primitiveType: PrimitiveType.PrimitiveTypeName,
                                       logicalTypeAnnotation: Option[LogicalTypeAnnotation],
                                       required: Boolean,
                                       length: Option[Int]
                                     ) extends SchemaDef {

  override type Self = PrimitiveSchemaDef

  override def apply(name: String): Type = {
    val builder = Types.primitive(
      primitiveType,
      if (required) Repetition.REQUIRED else Repetition.OPTIONAL
    )
    val withLogicalMetadata = logicalTypeAnnotation.foldLeft(builder)(_.as(_))
    val withLength = length.foldLeft(withLogicalMetadata)(_.length(_))

    withLength.named(name)
  }

  override def withRequired(required: Boolean): PrimitiveSchemaDef = this.copy(required = required)

}

object GroupSchemaDef {

  @deprecated("Use SchemaDef.group", since = "1.9.0")
  def required(fields: Type*): GroupSchemaDef = GroupSchemaDef(fields, required = true)
  @deprecated("Use SchemaDef.group", since = "1.9.0")
  def optional(fields: Type*): GroupSchemaDef = GroupSchemaDef(fields, required = false)

}

@deprecated("Use SchemaDef.group", since = "1.9.0")
case class GroupSchemaDef(fields: Seq[Type], required: Boolean) extends SchemaDef {

  override type Self = GroupSchemaDef

  override def apply(name: String): Type = {
    val builder = if (required) Types.requiredGroup() else Types.optionalGroup()
    builder.addFields(fields:_*).named(name)
  }

  override def withRequired(required: Boolean): GroupSchemaDef = this.copy(required = required)

}

object ListSchemaDef {

  val ElementName = "element"

  @deprecated("Use SchemaDef.list", since = "1.9.0")
  def required(elementSchemaDef: SchemaDef): ListSchemaDef = ListSchemaDef(elementSchemaDef(ElementName), required = true)
  @deprecated("Use SchemaDef.list", since = "1.9.0")
  def optional(elementSchemaDef: SchemaDef): ListSchemaDef = ListSchemaDef(elementSchemaDef(ElementName), required = false)

}

@deprecated("Use SchemaDef.list", since = "1.9.0")
case class ListSchemaDef(element: Type, required: Boolean) extends SchemaDef {

  override type Self = ListSchemaDef

  override def apply(name: String): Type = {
    val builder = if (required) Types.requiredList() else Types.optionalList()
    builder.element(element).named(name)
  }

  override def withRequired(required: Boolean): ListSchemaDef = this.copy(required = required)

}

object MapSchemaDef {

  val KeyName = "key"
  val ValueName = "value"

  @deprecated("Use SchemaDef.map", since = "1.9.0")
  def required(keySchemaDef: SchemaDef, valueSchemaDef: SchemaDef): MapSchemaDef = MapSchemaDef(
    keySchemaDef(KeyName), valueSchemaDef(ValueName), required = true
  )

  @deprecated("Use SchemaDef.map", since = "1.9.0")
  def optional(keySchemaDef: SchemaDef, valueSchemaDef: SchemaDef): MapSchemaDef = MapSchemaDef(
    keySchemaDef(KeyName), valueSchemaDef(ValueName), required = false
  )

}

@deprecated("Use SchemaDef.map", since = "1.9.0")
case class MapSchemaDef (key: Type, value: Type, required: Boolean) extends SchemaDef {

  override type Self = MapSchemaDef

  override def apply(name: String): Type = {
    val builder = if (required) Types.requiredMap() else Types.optionalMap()
    builder.key(key).value(value).named(name)
  }

  override def withRequired(required: Boolean): MapSchemaDef = this.copy(required = required)

}

trait SchemaDefs {

  @deprecated("Call SchemaDef#typed[V] in order to build TypedSchemaDef[V]", since = "1.9.0")
  def typedSchemaDef[V](schemaDef: SchemaDef): TypedSchemaDef[V] = schemaDef.asInstanceOf[TypedSchemaDef[V]]

  implicit val stringSchema: TypedSchemaDef[String] =
    SchemaDef.primitive(BINARY, required = false, logicalTypeAnnotation = Option(LogicalTypes.StringType)).typed[String]

  implicit val charSchema: TypedSchemaDef[Char] =
    SchemaDef.primitive(INT32, logicalTypeAnnotation = Option(LogicalTypes.Int32Type)).typed[Char]

  implicit val intSchema: TypedSchemaDef[Int] =
    SchemaDef.primitive(INT32, logicalTypeAnnotation = Option(LogicalTypes.Int32Type)).typed[Int]

  implicit val longSchema: TypedSchemaDef[Long] =
    SchemaDef.primitive(INT64, logicalTypeAnnotation = Option(LogicalTypes.Int64Type)).typed[Long]

  implicit val floatSchema: TypedSchemaDef[Float] =
    SchemaDef.primitive(FLOAT).typed[Float]

  implicit val doubleSchema: TypedSchemaDef[Double] =
    SchemaDef.primitive(DOUBLE).typed[Double]

  implicit val booleanSchema: TypedSchemaDef[Boolean] =
    SchemaDef.primitive(BOOLEAN).typed[Boolean]

  implicit val shortSchema: TypedSchemaDef[Short] =
    SchemaDef.primitive(INT32, logicalTypeAnnotation = Option(LogicalTypes.Int16Type)).typed[Short]

  implicit val byteSchema: TypedSchemaDef[Byte] =
    SchemaDef.primitive(INT32, logicalTypeAnnotation = Option(LogicalTypes.Int8Type)).typed[Byte]

  implicit val decimalSchema: TypedSchemaDef[BigDecimal] =
      SchemaDef.primitive(
        FIXED_LEN_BYTE_ARRAY,
        required = false,
        logicalTypeAnnotation = Option(LogicalTypes.DecimalType),
        length = Some(Decimals.ByteArrayLength)
      ).typed[BigDecimal]

  implicit val localDateSchema: TypedSchemaDef[java.time.LocalDate] =
    SchemaDef.primitive(INT32, required = false, logicalTypeAnnotation = Option(LogicalTypes.DateType)).typed[java.time.LocalDate]

  implicit val sqlDateSchema: TypedSchemaDef[java.sql.Date] =
    SchemaDef.primitive(INT32, required = false, logicalTypeAnnotation = Option(LogicalTypes.DateType)).typed[java.sql.Date]

  implicit val localDateTimeSchema: TypedSchemaDef[java.time.LocalDateTime] =
    SchemaDef.primitive(INT96, required = false).typed[java.time.LocalDateTime]

  implicit val sqlTimestampSchema: TypedSchemaDef[java.sql.Timestamp] =
    SchemaDef.primitive(INT96, required = false).typed[java.sql.Timestamp]

  implicit def productSchema[T](implicit parquetSchemaResolver: ParquetSchemaResolver[T]): TypedSchemaDef[T] =
    SchemaDef.group(parquetSchemaResolver.resolveSchema:_*).typed[T]

  implicit def optionSchema[T](implicit tSchemaDef: TypedSchemaDef[T]): TypedSchemaDef[Option[T]] =
    tSchemaDef.withRequired(false).typed[Option[T]]

  implicit def collectionSchema[E, Col[_]](implicit
                                           elementSchema: TypedSchemaDef[E],
                                           ev: Col[E] <:< Iterable[E]): TypedSchemaDef[Col[E]] =
     SchemaDef.list(elementSchema).typed[Col[E]]

  implicit def arraySchema[E, Col[_]](implicit
                                      elementSchema: TypedSchemaDef[E],
                                      ev: Col[E] =:= Array[E],
                                      classTag: ClassTag[E]): TypedSchemaDef[Col[E]] =
    if (classTag.runtimeClass == classOf[Byte])
      SchemaDef.primitive(BINARY, required = false).typed[Col[E]]
    else
      SchemaDef.list(elementSchema).typed[Col[E]]

  implicit def mapSchema[MapKey, MapValue](implicit
                                           keySchema: TypedSchemaDef[MapKey],
                                           valueSchema: TypedSchemaDef[MapValue]
                                          ): TypedSchemaDef[Map[MapKey, MapValue]] =
    // type of the map key must be required
    SchemaDef.map(keySchema.withRequired(true), valueSchema).typed[Map[MapKey, MapValue]]

}
