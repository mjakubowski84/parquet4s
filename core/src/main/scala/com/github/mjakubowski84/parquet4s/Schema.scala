package com.github.mjakubowski84.parquet4s

import com.github.mjakubowski84.parquet4s.SchemaDef.Meta
import org.apache.parquet.schema.*
import org.apache.parquet.schema.LogicalTypeAnnotation.{
  DateLogicalTypeAnnotation,
  DecimalLogicalTypeAnnotation,
  IntLogicalTypeAnnotation,
  StringLogicalTypeAnnotation,
  TimestampLogicalTypeAnnotation
}
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.*
import org.apache.parquet.schema.Type.Repetition

import scala.annotation.nowarn
import scala.reflect.ClassTag
import scala.jdk.CollectionConverters.*

object Message {

  val DefaultName = "parquet4s_schema"

  def apply(name: Option[String], fields: Type*): MessageType =
    Types.buildMessage().addFields(fields*).named(name.getOrElse(DefaultName))

  /** Merges the fields before creating a schema. Merge is done by unifying types of columns that are defined in a
    * projection more than once. The first mentioned column of primitive type is chosen in the case of duplicates. Union
    * of of member fields is executed in the case of complex types.
    * @param fields
    *   fields to be merged and then used for defining the schema
    * @return
    *   schema created
    */
  private[parquet4s] def merge(fields: Seq[Type]): MessageType =
    Message(name = None, fields = mergeFields(fields)*)

  private def mergeFields(fields: Seq[Type]): Seq[Type] =
    fields
      .foldLeft(Map.empty[String, Type], Vector.empty[Type]) { case ((register, merged), tpe) =>
        val fieldName = tpe.getName
        register.get(fieldName) match {
          case Some(group: GroupType) if !tpe.isPrimitive() =>
            val newMemberFields = mergeFields(group.getFields().asScala.toSeq ++ tpe.asGroupType().getFields().asScala)
            val mergedGroup     = group.withNewFields(newMemberFields.asJava)
            register.updated(fieldName, mergedGroup) -> (merged.filterNot(_ == group) :+ mergedGroup)
          case Some(firstSeen) =>
            register -> (merged :+ firstSeen)
          case None =>
            register.updated(fieldName, tpe) -> (merged :+ tpe)
        }
      }
      ._2

}

/** Defines Parquet schema. Produces Parquet [[org.apache.parquet.schema.Type]] for a field of given name.
  */
trait SchemaDef {

  type Self <: SchemaDef

  def apply(name: String): Type

  def withRequired(required: Boolean): Self

  def typed[V]: TypedSchemaDef[V] = TypedSchemaDef.wrap[V](this)

  private[parquet4s] def metadata: Set[SchemaDef.Meta.Property]

  private[parquet4s] def withMetadata(meta: SchemaDef.Meta.Property): Self

}

object SchemaDef {

  private[parquet4s] object Meta {
    sealed trait Property
    case object Generated extends Property
  }

  def primitive(
      primitiveType: PrimitiveType.PrimitiveTypeName,
      logicalTypeAnnotation: Option[LogicalTypeAnnotation] = None,
      required: Boolean                                    = true,
      length: Option[Int]                                  = None
  ): SchemaDef =
    PrimitiveSchemaDef(primitiveType, logicalTypeAnnotation, required, length, Set.empty)

  def group(fields: Type*): SchemaDef =
    GroupSchemaDef(fields, required = false, metadata = Set.empty)

  def list(elementSchemaDef: SchemaDef): SchemaDef =
    ListSchemaDef(elementSchemaDef(ListSchemaDef.ElementName), required = false, metadata = Set.empty)

  def map(keySchemaDef: SchemaDef, valueSchemaDef: SchemaDef): SchemaDef = MapSchemaDef(
    keySchemaDef(MapSchemaDef.KeyName),
    valueSchemaDef(MapSchemaDef.ValueName),
    required = false,
    metadata = Set.empty
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
  val TimestampMillisType: TimestampLogicalTypeAnnotation = LogicalTypeAnnotation.timestampType(
    true,
    LogicalTypeAnnotation.TimeUnit.MILLIS
  )
  val TimestampMicrosType: TimestampLogicalTypeAnnotation = LogicalTypeAnnotation.timestampType(
    true,
    LogicalTypeAnnotation.TimeUnit.MICROS
  )
  val TimestampNanosType: TimestampLogicalTypeAnnotation = LogicalTypeAnnotation.timestampType(
    true,
    LogicalTypeAnnotation.TimeUnit.NANOS
  )
}

private case class PrimitiveSchemaDef(
    primitiveType: PrimitiveType.PrimitiveTypeName,
    logicalTypeAnnotation: Option[LogicalTypeAnnotation],
    required: Boolean,
    length: Option[Int],
    metadata: Set[SchemaDef.Meta.Property]
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

  override def withMetadata(meta: SchemaDef.Meta.Property): PrimitiveSchemaDef = this.copy(metadata = metadata + meta)

}

private case class GroupSchemaDef(fields: Seq[Type], required: Boolean, metadata: Set[SchemaDef.Meta.Property])
    extends SchemaDef {

  override type Self = GroupSchemaDef

  override def apply(name: String): Type = {
    val builder = if (required) Types.requiredGroup() else Types.optionalGroup()
    builder.addFields(fields*).named(name)
  }

  override def withRequired(required: Boolean): GroupSchemaDef = this.copy(required = required)

  override def withMetadata(meta: SchemaDef.Meta.Property): GroupSchemaDef = this.copy(metadata = metadata + meta)

}

private object ListSchemaDef {
  val ElementName = "element"
}

private case class ListSchemaDef(element: Type, required: Boolean, metadata: Set[SchemaDef.Meta.Property])
    extends SchemaDef {

  override type Self = ListSchemaDef

  override def apply(name: String): Type = {
    val builder = if (required) Types.requiredList() else Types.optionalList()
    builder.element(element).named(name)
  }

  override def withRequired(required: Boolean): ListSchemaDef = this.copy(required = required)

  override def withMetadata(meta: SchemaDef.Meta.Property): ListSchemaDef = this.copy(metadata = metadata + meta)

}

private object MapSchemaDef {
  val KeyName   = "key"
  val ValueName = "value"
}

private case class MapSchemaDef(key: Type, value: Type, required: Boolean, metadata: Set[SchemaDef.Meta.Property])
    extends SchemaDef {

  override type Self = MapSchemaDef

  override def apply(name: String): Type = {
    val builder = if (required) Types.requiredMap() else Types.optionalMap()
    builder.key(key).value(value).named(name)
  }

  override def withRequired(required: Boolean): MapSchemaDef = this.copy(required = required)

  override def withMetadata(meta: SchemaDef.Meta.Property): MapSchemaDef = this.copy(metadata = metadata + meta)

}

/** [[SchemaDef]] for type `V`.
  */
trait TypedSchemaDef[V] extends SchemaDef {
  private[parquet4s] def isGroup: Boolean
}

object TypedSchemaDef extends PrimitiveSchemaDefs with TimeValueSchemaDefs with ComplexSchemaDefs {
  private[parquet4s] def wrap[V](schemaDef: SchemaDef): TypedSchemaDef[V] =
    new TypedSchemaDef[V] {
      override val isGroup: Boolean = schemaDef match {
        case typed: TypedSchemaDef[?] => typed.isGroup
        case _: GroupSchemaDef        => true
        case _                        => false
      }
      override type Self = TypedSchemaDef[V]
      override def apply(name: String): Type                                 = schemaDef.apply(name)
      override def withRequired(required: Boolean): Self                     = wrap[V](schemaDef.withRequired(required))
      override private[parquet4s] val metadata: Set[SchemaDef.Meta.Property] = schemaDef.metadata
      override private[parquet4s] def withMetadata(meta: Meta.Property): Self =
        wrap[V](schemaDef.withMetadata(meta))
    }
}

trait PrimitiveSchemaDefs {

  implicit val stringSchema: TypedSchemaDef[String] =
    SchemaDef
      .primitive(BINARY, required = false, logicalTypeAnnotation = Option(LogicalTypes.StringType))
      .withMetadata(SchemaDef.Meta.Generated)
      .typed[String]

  implicit val charSchema: TypedSchemaDef[Char] =
    SchemaDef
      .primitive(INT32, logicalTypeAnnotation = Option(LogicalTypes.Int32Type))
      .withMetadata(SchemaDef.Meta.Generated)
      .typed[Char]

  implicit val intSchema: TypedSchemaDef[Int] =
    SchemaDef
      .primitive(INT32, logicalTypeAnnotation = Option(LogicalTypes.Int32Type))
      .withMetadata(SchemaDef.Meta.Generated)
      .typed[Int]

  implicit val longSchema: TypedSchemaDef[Long] =
    SchemaDef
      .primitive(INT64, logicalTypeAnnotation = Option(LogicalTypes.Int64Type))
      .withMetadata(SchemaDef.Meta.Generated)
      .typed[Long]

  implicit val floatSchema: TypedSchemaDef[Float] =
    SchemaDef.primitive(FLOAT).withMetadata(SchemaDef.Meta.Generated).typed[Float]

  implicit val doubleSchema: TypedSchemaDef[Double] =
    SchemaDef.primitive(DOUBLE).withMetadata(SchemaDef.Meta.Generated).typed[Double]

  implicit val booleanSchema: TypedSchemaDef[Boolean] =
    SchemaDef.primitive(BOOLEAN).withMetadata(SchemaDef.Meta.Generated).typed[Boolean]

  implicit val shortSchema: TypedSchemaDef[Short] =
    SchemaDef
      .primitive(INT32, logicalTypeAnnotation = Option(LogicalTypes.Int16Type))
      .withMetadata(SchemaDef.Meta.Generated)
      .typed[Short]

  implicit val byteSchema: TypedSchemaDef[Byte] =
    SchemaDef
      .primitive(INT32, logicalTypeAnnotation = Option(LogicalTypes.Int8Type))
      .withMetadata(SchemaDef.Meta.Generated)
      .typed[Byte]

  implicit val decimalSchema: TypedSchemaDef[BigDecimal] =
    SchemaDef
      .primitive(
        FIXED_LEN_BYTE_ARRAY,
        required              = false,
        logicalTypeAnnotation = Option(LogicalTypes.DecimalType),
        length                = Some(Decimals.ByteArrayLength)
      )
      .withMetadata(SchemaDef.Meta.Generated)
      .typed[BigDecimal]
}

trait TimeValueSchemaDefs {
  implicit val localDateSchema: TypedSchemaDef[java.time.LocalDate] =
    SchemaDef
      .primitive(INT32, required = false, logicalTypeAnnotation = Option(LogicalTypes.DateType))
      .withMetadata(SchemaDef.Meta.Generated)
      .typed[java.time.LocalDate]

  implicit val sqlDateSchema: TypedSchemaDef[java.sql.Date] =
    SchemaDef
      .primitive(INT32, required = false, logicalTypeAnnotation = Option(LogicalTypes.DateType))
      .withMetadata(SchemaDef.Meta.Generated)
      .typed[java.sql.Date]

  implicit val localDateTimeSchema: TypedSchemaDef[java.time.LocalDateTime] =
    SchemaDef.primitive(INT96, required = false).withMetadata(SchemaDef.Meta.Generated).typed[java.time.LocalDateTime]

  implicit val instantSchema: TypedSchemaDef[java.time.Instant] =
    SchemaDef.primitive(INT96, required = false).withMetadata(SchemaDef.Meta.Generated).typed[java.time.Instant]

  implicit val sqlTimestampSchema: TypedSchemaDef[java.sql.Timestamp] =
    SchemaDef.primitive(INT96, required = false).withMetadata(SchemaDef.Meta.Generated).typed[java.sql.Timestamp]
}

trait ComplexSchemaDefs extends ProductSchemaDefs {
  implicit def optionSchema[T](implicit tSchemaDef: TypedSchemaDef[T]): TypedSchemaDef[Option[T]] =
    tSchemaDef.withRequired(false).typed[Option[T]]

  implicit def collectionSchema[E, Col[_]](implicit
      elementSchema: TypedSchemaDef[E],
      @nowarn ev: Col[E] <:< Iterable[E]
  ): TypedSchemaDef[Col[E]] =
    SchemaDef.list(elementSchema).withMetadata(SchemaDef.Meta.Generated).typed[Col[E]]

  implicit def arraySchema[E, Col[_]](implicit
      elementSchema: TypedSchemaDef[E],
      @nowarn ev: Col[E] =:= Array[E],
      classTag: ClassTag[E]
  ): TypedSchemaDef[Col[E]] =
    if (classTag.runtimeClass == classOf[Byte])
      SchemaDef.primitive(BINARY, required = false).withMetadata(SchemaDef.Meta.Generated).typed[Col[E]]
    else
      SchemaDef.list(elementSchema).withMetadata(SchemaDef.Meta.Generated).typed[Col[E]]

  implicit def mapSchema[MapKey, MapValue](implicit
      keySchema: TypedSchemaDef[MapKey],
      valueSchema: TypedSchemaDef[MapValue]
  ): TypedSchemaDef[Map[MapKey, MapValue]] =
    // type of the map key must be required
    SchemaDef
      .map(keySchema.withRequired(true), valueSchema)
      .withMetadata(SchemaDef.Meta.Generated)
      .typed[Map[MapKey, MapValue]]
}
