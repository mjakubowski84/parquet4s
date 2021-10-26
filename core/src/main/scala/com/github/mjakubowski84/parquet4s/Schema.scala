package com.github.mjakubowski84.parquet4s

import com.github.mjakubowski84.parquet4s.ParquetSchemaResolver.TypedSchemaDef
import org.apache.parquet.schema.LogicalTypeAnnotation.{
  DateLogicalTypeAnnotation,
  DecimalLogicalTypeAnnotation,
  IntLogicalTypeAnnotation,
  StringLogicalTypeAnnotation
}
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.{
  BINARY,
  BOOLEAN,
  DOUBLE,
  FIXED_LEN_BYTE_ARRAY,
  FLOAT,
  INT32,
  INT64,
  INT96
}
import org.apache.parquet.schema.Type.Repetition
import org.apache.parquet.schema.{LogicalTypeAnnotation, MessageType, PrimitiveType, Type, Types}

import scala.reflect.ClassTag

import scala.language.higherKinds

object Message {

  val DefaultName = "parquet4s_schema"

  def apply(name: Option[String], fields: Type*): MessageType =
    Types.buildMessage().addFields(fields: _*).named(name.getOrElse(DefaultName))

}

trait SchemaDef {

  type Self <: SchemaDef

  def apply(name: String): Type

  def withRequired(required: Boolean): Self

  def typed[V]: ParquetSchemaResolver.TypedSchemaDef[V] = this.asInstanceOf[TypedSchemaDef[V]]

}

object SchemaDef {
  def primitive(
      primitiveType: PrimitiveType.PrimitiveTypeName,
      logicalTypeAnnotation: Option[LogicalTypeAnnotation] = None,
      required: Boolean                                    = true,
      length: Option[Int]                                  = None
  ): SchemaDef =
    PrimitiveSchemaDef(primitiveType, logicalTypeAnnotation, required, length)

  def group(fields: Type*): SchemaDef =
    GroupSchemaDef(fields, required = false)

  def list(elementSchemaDef: SchemaDef): SchemaDef =
    ListSchemaDef(elementSchemaDef(ListSchemaDef.ElementName), required = false)

  def map(keySchemaDef: SchemaDef, valueSchemaDef: SchemaDef): SchemaDef = MapSchemaDef(
    keySchemaDef(MapSchemaDef.KeyName),
    valueSchemaDef(MapSchemaDef.ValueName),
    required = false
  )
}

object LogicalTypes {
  val Int64Type: IntLogicalTypeAnnotation       = LogicalTypeAnnotation.intType(64, true)
  val Int32Type: IntLogicalTypeAnnotation       = LogicalTypeAnnotation.intType(32, true)
  val Int16Type: IntLogicalTypeAnnotation       = LogicalTypeAnnotation.intType(16, true)
  val Int8Type: IntLogicalTypeAnnotation        = LogicalTypeAnnotation.intType(8, true)
  val DecimalType: DecimalLogicalTypeAnnotation = LogicalTypeAnnotation.decimalType(Decimals.Scale, Decimals.Precision)
  val StringType: StringLogicalTypeAnnotation   = LogicalTypeAnnotation.stringType()
  val DateType: DateLogicalTypeAnnotation       = LogicalTypeAnnotation.dateType()
}

private case class PrimitiveSchemaDef(
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
    val withLength          = length.foldLeft(withLogicalMetadata)(_.length(_))

    withLength.named(name)
  }

  override def withRequired(required: Boolean): PrimitiveSchemaDef = this.copy(required = required)

}

private case class GroupSchemaDef(fields: Seq[Type], required: Boolean) extends SchemaDef {

  override type Self = GroupSchemaDef

  override def apply(name: String): Type = {
    val builder = if (required) Types.requiredGroup() else Types.optionalGroup()
    builder.addFields(fields: _*).named(name)
  }

  override def withRequired(required: Boolean): GroupSchemaDef = this.copy(required = required)

}

private object ListSchemaDef {
  val ElementName = "element"
}

private case class ListSchemaDef(element: Type, required: Boolean) extends SchemaDef {

  override type Self = ListSchemaDef

  override def apply(name: String): Type = {
    val builder = if (required) Types.requiredList() else Types.optionalList()
    builder.element(element).named(name)
  }

  override def withRequired(required: Boolean): ListSchemaDef = this.copy(required = required)

}

private object MapSchemaDef {
  val KeyName   = "key"
  val ValueName = "value"
}

private case class MapSchemaDef(key: Type, value: Type, required: Boolean) extends SchemaDef {

  override type Self = MapSchemaDef

  override def apply(name: String): Type = {
    val builder = if (required) Types.requiredMap() else Types.optionalMap()
    builder.key(key).value(value).named(name)
  }

  override def withRequired(required: Boolean): MapSchemaDef = this.copy(required = required)

}

trait SchemaDefs {

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
    SchemaDef
      .primitive(
        FIXED_LEN_BYTE_ARRAY,
        required              = false,
        logicalTypeAnnotation = Option(LogicalTypes.DecimalType),
        length                = Some(Decimals.ByteArrayLength)
      )
      .typed[BigDecimal]

  implicit val localDateSchema: TypedSchemaDef[java.time.LocalDate] =
    SchemaDef
      .primitive(INT32, required = false, logicalTypeAnnotation = Option(LogicalTypes.DateType))
      .typed[java.time.LocalDate]

  implicit val sqlDateSchema: TypedSchemaDef[java.sql.Date] =
    SchemaDef
      .primitive(INT32, required = false, logicalTypeAnnotation = Option(LogicalTypes.DateType))
      .typed[java.sql.Date]

  implicit val localDateTimeSchema: TypedSchemaDef[java.time.LocalDateTime] =
    SchemaDef.primitive(INT96, required = false).typed[java.time.LocalDateTime]

  implicit val sqlTimestampSchema: TypedSchemaDef[java.sql.Timestamp] =
    SchemaDef.primitive(INT96, required = false).typed[java.sql.Timestamp]

  implicit def productSchema[T](implicit parquetSchemaResolver: ParquetSchemaResolver[T]): TypedSchemaDef[T] =
    SchemaDef.group(parquetSchemaResolver.resolveSchema(Cursor.simple): _*).typed[T]

  implicit def optionSchema[T](implicit tSchemaDef: TypedSchemaDef[T]): TypedSchemaDef[Option[T]] =
    tSchemaDef.withRequired(false).typed[Option[T]]

  implicit def collectionSchema[E, Col[_]](implicit
      elementSchema: TypedSchemaDef[E],
      ev: Col[E] <:< Iterable[E]
  ): TypedSchemaDef[Col[E]] =
    SchemaDef.list(elementSchema).typed[Col[E]]

  implicit def arraySchema[E, Col[_]](implicit
      elementSchema: TypedSchemaDef[E],
      ev: Col[E] =:= Array[E],
      classTag: ClassTag[E]
  ): TypedSchemaDef[Col[E]] =
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
