package com.github.mjakubowski84.parquet4s

import org.apache.parquet.schema.{MessageType, Type}
import shapeless.labelled.FieldType
import shapeless.{::, HList, HNil, LabelledGeneric, Lazy, Witness}

trait SkippingParquetSchemaResolver[T] {

  def resolveSchema(cursor: Cursor): List[Type]

}

object SkippingParquetSchemaResolver
  extends SchemaDefs {

  class TypedSchemaDefInvoker[K <: Symbol : Witness.Aux, V](schemaDef: TypedSchemaDef[V]) {
    private def fieldName = implicitly[Witness.Aux[K]].value.name
    def invoke(): Type = schemaDef(fieldName)
    def invokeOther(otherSchemaDef: TypedSchemaDef[V]): Type = otherSchemaDef(fieldName)
  }

  def resolveSchema[T](toSkip: Iterable[String])(implicit g: SkippingParquetSchemaResolver[T]): MessageType =
    Message(g.resolveSchema(Cursor.skipping(toSkip)):_*)

  implicit val hnil: SkippingParquetSchemaResolver[HNil] = new SkippingParquetSchemaResolver[HNil] {
    def resolveSchema(cursor: Cursor): List[Type] = List.empty
  }

  implicit def hcons[K <: Symbol, V, T <: HList](implicit
                                                           witness: Witness.Aux[K],
                                                           schemaDef: TypedSchemaDef[V],
                                                           visitor: SchemaVisitor[K, V] = defaultSchemaVisitor[K, V],
                                                           rest: SkippingParquetSchemaResolver[T]
                                                ): SkippingParquetSchemaResolver[FieldType[K, V] :: T] =
    new SkippingParquetSchemaResolver[FieldType[K, V] :: T] {
      def resolveSchema(cursor: Cursor): List[Type] = {
        cursor.advance[K] match {
          case Some(newCursor) =>
            newCursor.accept(new TypedSchemaDefInvoker(schemaDef), visitor) match {
              case Some(head) =>
                head +: rest.resolveSchema(cursor)
              case None =>
                rest.resolveSchema(cursor)
            }
          case None =>
            rest.resolveSchema(cursor)
        }
      }
    }

  implicit def generic[T, G](implicit
                             lg: LabelledGeneric.Aux[T, G],
                             rest: Lazy[SkippingParquetSchemaResolver[G]]
                            ): SkippingParquetSchemaResolver[T] = new SkippingParquetSchemaResolver[T] {
    def resolveSchema(cursor: Cursor): List[Type] = rest.value.resolveSchema(cursor)
  }

  trait SchemaVisitor[K <: Symbol, V] extends Cursor.Visitor[TypedSchemaDefInvoker[K, V], Option[Type]] {
    override def onCompleted(cursor: Cursor, invoker: TypedSchemaDefInvoker[K, V]): Option[Type] =
      throw new UnsupportedOperationException("Skipping cursor does not complete.")
  }

  def defaultSchemaVisitor[K <: Symbol, V]: SchemaVisitor[K, V] = new SchemaVisitor[K, V] {
    override def onActive(cursor: Cursor, invoker: TypedSchemaDefInvoker[K, V]): Option[Type] =
      Option(invoker.invoke())
  }

  implicit def productSchemaVisitor[K <: Symbol, V](implicit resolver: SkippingParquetSchemaResolver[V]): SchemaVisitor[K, V] =
    new SchemaVisitor[K, V] {
      override def onActive(cursor: Cursor, invoker: TypedSchemaDefInvoker[K, V]): Option[Type] =
        resolver.resolveSchema(cursor) match {
          case Nil =>
            None
          case list =>
            Some(invoker.invokeOther(typedSchemaDef[V](
              GroupSchemaDef.optional(list:_*)
            )))
        }
    }

}
