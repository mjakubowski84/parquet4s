package com.github.mjakubowski84.parquet4s

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

  def name: String
  /**
    * @return list of [[org.apache.parquet.schema.Type]] for each product element that <i>T</i> contains.
    */
  def resolveSchema: List[Type]

}

object ParquetSchemaResolver
  extends SchemaDefs {

  /**
    * Builds full Parquet file schema ([[org.apache.parquet.schema.MessageType]]) from <i>T</i>.
    */
  def resolveSchema[T](implicit g: ParquetSchemaResolver[T]): MessageType = Message(g.name, g.resolveSchema:_*)

  implicit val hnil: ParquetSchemaResolver[HNil] = new ParquetSchemaResolver[HNil] {
    def name: String = ???
    def resolveSchema: List[Type] = List.empty
  }

  implicit def hcons[K <: Symbol, V, T <: HList](implicit
                                                 witness: Witness.Aux[K],
                                                 schemaDef: TypedSchemaDef[V],
                                                 rest: ParquetSchemaResolver[T]
                                                ): ParquetSchemaResolver[FieldType[K, V] :: T] =
    new ParquetSchemaResolver[FieldType[K, V] :: T] {
      def name: String = ???
      def resolveSchema: List[Type] = schemaDef(witness.value.name) +: rest.resolveSchema
    }

  implicit def generic[T, G](implicit
                             ct: ClassTag[T],
                             lg: LabelledGeneric.Aux[T, G],
                             rest: Lazy[ParquetSchemaResolver[G]]
                            ): ParquetSchemaResolver[T] = new ParquetSchemaResolver[T] {
    def name: String = ct.runtimeClass.getCanonicalName
    def resolveSchema: List[Type] = rest.value.resolveSchema
  }
}

object Message {

  def apply(name: String, fields: Type*): MessageType = Types.buildMessage()
                                                             .addFields(fields:_*)
                                                             .named(Option(name)
                                                                      .getOrElse("parquet4s_schema"))
}

trait SchemaDef {

  type Self <: SchemaDef

  def apply(name: String): Type

  def withRequired(required: Boolean): Self

}

case class PrimitiveSchemaDef(
                              primitiveType: PrimitiveType.PrimitiveTypeName,
                              required: Boolean = true,
                              originalType: Option[OriginalType] = None,
                              precision: Option[Int] = None,
                              scale: Option[Int] = None,
                              length: Option[Int] = None
                             ) extends SchemaDef {

  override type Self = PrimitiveSchemaDef

  override def apply(name: String): Type = {
    val builder = Types.primitive(
      primitiveType,
      if (required) Repetition.REQUIRED else Repetition.OPTIONAL
    )
    val withOriginalType = originalType.foldLeft(builder)(_.as(_))
    val withPrecision = precision.foldLeft(withOriginalType)(_.precision(_))
    val withScale = scale.foldLeft(withPrecision)(_.scale(_))
    val withLength = length.foldLeft(withScale)(_.length(_))
    
    withLength.named(name)
  }

  override def withRequired(required: Boolean): PrimitiveSchemaDef = this.copy(required = required)

}

object GroupSchemaDef {

  def required(fields: Type*): GroupSchemaDef = GroupSchemaDef(fields, required = true)
  def optional(fields: Type*): GroupSchemaDef = GroupSchemaDef(fields, required = false)

}

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

  def required(elementSchemaDef: SchemaDef): ListSchemaDef = ListSchemaDef(elementSchemaDef(ElementName), required = true)
  def optional(elementSchemaDef: SchemaDef): ListSchemaDef = ListSchemaDef(elementSchemaDef(ElementName), required = false)

}

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

  def required(keySchemaDef: SchemaDef, valueSchemaDef: SchemaDef): MapSchemaDef = new MapSchemaDef(
    keySchemaDef(KeyName), valueSchemaDef(ValueName), required = true
  )

  def optional(keySchemaDef: SchemaDef, valueSchemaDef: SchemaDef): MapSchemaDef = new MapSchemaDef(
    keySchemaDef(KeyName), valueSchemaDef(ValueName), required = false
  )

}

case class MapSchemaDef(key: Type, value: Type, required: Boolean) extends SchemaDef {
  
  override type Self = MapSchemaDef
  
  override def apply(name: String): Type = {
    val builder = if (required) Types.requiredMap() else Types.optionalMap()
    builder.key(key).value(value).named(name)
  }

  override def withRequired(required: Boolean): MapSchemaDef = this.copy(required = required)

}

trait SchemaDefs {

  trait Tag[V]
  type TypedSchemaDef[V] = SchemaDef with Tag[V]

  def typedSchemaDef[V](schemaDef: SchemaDef): TypedSchemaDef[V] = schemaDef.asInstanceOf[TypedSchemaDef[V]]

  implicit val stringSchema: TypedSchemaDef[String] =
    typedSchemaDef[String](
      PrimitiveSchemaDef(PrimitiveType.PrimitiveTypeName.BINARY, required = false, originalType = Some(OriginalType.UTF8))
    )

  implicit val charSchema: TypedSchemaDef[Char] =
    typedSchemaDef[Char](
      PrimitiveSchemaDef(PrimitiveType.PrimitiveTypeName.INT32, originalType = Some(OriginalType.INT_32))
    )

  implicit val intSchema: TypedSchemaDef[Int] =
    typedSchemaDef[Int](
      PrimitiveSchemaDef(PrimitiveType.PrimitiveTypeName.INT32, originalType = Some(OriginalType.INT_32))
    )

  implicit val longSchema: TypedSchemaDef[Long] =
    typedSchemaDef[Long](
      PrimitiveSchemaDef(PrimitiveType.PrimitiveTypeName.INT64, originalType = Some(OriginalType.INT_64))
    )

  implicit val floatSchema: TypedSchemaDef[Float] =
    typedSchemaDef[Float](
      PrimitiveSchemaDef(PrimitiveType.PrimitiveTypeName.FLOAT)
    )

  implicit val doubleSchema: TypedSchemaDef[Double] =
    typedSchemaDef[Double](
      PrimitiveSchemaDef(PrimitiveType.PrimitiveTypeName.DOUBLE)
    )

  implicit val booleanSchema: TypedSchemaDef[Boolean] =
    typedSchemaDef[Boolean](
      PrimitiveSchemaDef(PrimitiveType.PrimitiveTypeName.BOOLEAN)
    )

  implicit val shortSchema: TypedSchemaDef[Short] =
    typedSchemaDef[Short](
      PrimitiveSchemaDef(PrimitiveType.PrimitiveTypeName.INT32, originalType = Some(OriginalType.INT_16))
    )

  implicit val byteSchema: TypedSchemaDef[Byte] =
    typedSchemaDef[Byte](
      PrimitiveSchemaDef(PrimitiveType.PrimitiveTypeName.INT32, originalType = Some(OriginalType.INT_8))
    )
    
  implicit val decimalSchema: TypedSchemaDef[BigDecimal] =
    typedSchemaDef[BigDecimal](
      PrimitiveSchemaDef(
        PrimitiveType.PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY,
        required = false,
        originalType = Some(OriginalType.DECIMAL),
        precision = Some(Decimals.Precision),
        scale = Some(Decimals.Scale),
        length = Some(Decimals.ByteArrayLength)
      )
    )

  implicit val localDateSchema: TypedSchemaDef[java.time.LocalDate] =
    typedSchemaDef[java.time.LocalDate](
      PrimitiveSchemaDef(PrimitiveType.PrimitiveTypeName.INT32, required = false, originalType = Some(OriginalType.DATE))
    )

  implicit val sqlDateSchema: TypedSchemaDef[java.sql.Date] =
    typedSchemaDef[java.sql.Date](
      PrimitiveSchemaDef(PrimitiveType.PrimitiveTypeName.INT32, required = false, originalType = Some(OriginalType.DATE))
    )

  implicit val localDateTimeSchema: TypedSchemaDef[java.time.LocalDateTime] =
    typedSchemaDef[java.time.LocalDateTime](
      PrimitiveSchemaDef(PrimitiveType.PrimitiveTypeName.INT96, required = false)
    )

  implicit val sqlTimestampSchema: TypedSchemaDef[java.sql.Timestamp] =
    typedSchemaDef[java.sql.Timestamp](
      PrimitiveSchemaDef(PrimitiveType.PrimitiveTypeName.INT96, required = false)
    )

  implicit def productSchema[T](implicit parquetSchemaResolver: ParquetSchemaResolver[T]): TypedSchemaDef[T] =
    typedSchemaDef[T](
      GroupSchemaDef.optional(parquetSchemaResolver.resolveSchema:_*)
    )

  implicit def optionSchema[T](implicit tSchemaDef: TypedSchemaDef[T]): TypedSchemaDef[Option[T]] =
    typedSchemaDef[Option[T]](
      tSchemaDef.withRequired(false)
    )

  implicit def collectionSchema[E, Col[_]](implicit
                                           elementSchema: TypedSchemaDef[E],
                                           ev: Col[E] <:< Iterable[E]): TypedSchemaDef[Col[E]] =
    typedSchemaDef[Col[E]](
      ListSchemaDef.optional(elementSchema)
    )

  implicit def arraySchema[E, Col[_]](implicit
                                      elementSchema: TypedSchemaDef[E],
                                      ev: Col[E] =:= Array[E],
                                      classTag: ClassTag[E]): TypedSchemaDef[Col[E]] =
    if (classTag.runtimeClass == classOf[Byte])
      typedSchemaDef[Col[E]](
        PrimitiveSchemaDef(PrimitiveType.PrimitiveTypeName.BINARY, required = false)
      )
    else
      typedSchemaDef[Col[E]](
        ListSchemaDef.optional(elementSchema)
      )

  implicit def mapSchema[MapKey, MapValue](implicit
                                           keySchema: TypedSchemaDef[MapKey],
                                           valueSchema: TypedSchemaDef[MapValue]
                                          ): TypedSchemaDef[Map[MapKey, MapValue]] =
    typedSchemaDef[Map[MapKey, MapValue]] {
      // type of the map key must be required
      MapSchemaDef.optional(keySchema.withRequired(true), valueSchema)
    }

}
