package com.github.mjakubowski84.parquet4s

import org.apache.parquet.schema.Type.Repetition
import org.apache.parquet.schema._
import shapeless._
import shapeless.labelled._

import scala.collection.JavaConverters._
import scala.language.higherKinds


object ParquetSchemaResolver
  extends Schemas {

  sealed trait SchemaResolver[T] {

    def resolveSchema: List[Type]

  }

  def resolveSchema[T](implicit g: SchemaResolver[T]): MessageType = new MessageType("parquet4s-schema", g.resolveSchema.asJava)

  implicit val hnil: SchemaResolver[HNil] = new SchemaResolver[HNil] {
    def resolveSchema: List[Type] = List.empty
  }

  implicit def hcons[K <: Symbol, V, T <: HList](implicit
                                                 witness: Witness.Aux[K],
                                                 schemaForName: SchemaForName[V],
                                                 rest: SchemaResolver[T]
                                                ): SchemaResolver[FieldType[K, V] :: T] = new SchemaResolver[FieldType[K, V] :: T] {
    def resolveSchema: List[Type] = schemaForName(witness.value.name) +: rest.resolveSchema
  }

  implicit def caseClass[T, G](implicit
                               lg: LabelledGeneric.Aux[T, G],
                               rest: SchemaResolver[G]
                              ): SchemaResolver[T] = new SchemaResolver[T] {
    def resolveSchema: List[Type] = rest.resolveSchema
  }
}

trait Schemas {

  trait SchemaTag[V]
  type Schema[V] = Type with SchemaTag[V]
  type SchemaForName[V] = String => Schema[V]

  def schema[V](`type`: Type): Schema[V] = `type`.asInstanceOf[Schema[V]]

  implicit def stringSchema: SchemaForName[String] = name =>
    schema[String](
      Types.primitive(PrimitiveType.PrimitiveTypeName.BINARY, Repetition.OPTIONAL).as(OriginalType.UTF8).named(name)
    )

  implicit def intSchema: SchemaForName[Int] = name =>
    schema[Int](
      Types.primitive(PrimitiveType.PrimitiveTypeName.INT32, Repetition.REQUIRED).as(OriginalType.INT_32).named(name)
    )

  implicit def longSchema: SchemaForName[Long] = name =>
    schema[Long](
      Types.primitive(PrimitiveType.PrimitiveTypeName.INT64, Repetition.REQUIRED).as(OriginalType.INT_64).named(name)
    )

  implicit def floatSchema: SchemaForName[Float] = name =>
    schema[Float](
      Types.primitive(PrimitiveType.PrimitiveTypeName.FLOAT, Repetition.REQUIRED).named(name)
    )

  implicit def doubleSchema: SchemaForName[Double] = name =>
    schema[Double](
      Types.primitive(PrimitiveType.PrimitiveTypeName.DOUBLE, Repetition.REQUIRED).named(name)
    )

  implicit def booleanSchema: SchemaForName[Boolean] = name =>
    schema[Boolean](
      Types.primitive(PrimitiveType.PrimitiveTypeName.BOOLEAN, Repetition.REQUIRED).named(name)
    )

  implicit def optionSchema[T](implicit schemaForT: SchemaForName[T]): SchemaForName[Option[T]] = name => {
    schemaForT(name) match {
      case tSchema if tSchema.isPrimitive =>
        val s = tSchema.asPrimitiveType()
        schema[Option[T]](
          Types.optional(s.getPrimitiveTypeName)
            .as(s.getOriginalType)
            .precision(Option(s.getDecimalMetadata).map(_.getPrecision).getOrElse(0))
            .scale(Option(s.getDecimalMetadata).map(_.getScale).getOrElse(0))
            .columnOrder(s.columnOrder())
            .named(name)
        )
      case tSchema =>
        val s = tSchema.asGroupType()
        schema[Option[T]](
          Types.optionalGroup()
            .as(s.getOriginalType)
            .addFields(s.getFields.asScala:_*)
            .named(name)
        )
    }
  }

  implicit def traversableSchema[E, X[_] <: Traversable[_]](implicit
                                       elementSchema: SchemaForName[E]
                                      ): SchemaForName[X[E]] = name =>
    schema[X[E]](
      Types.optionalList().element(elementSchema("element")).named(name)
    )

  implicit def arraySchema[E](implicit
                              elementSchema: SchemaForName[E]
                             ): SchemaForName[Array[E]] = name =>
    schema[Array[E]](
      Types.optionalList().element(elementSchema("element")).named(name)
    )

}
