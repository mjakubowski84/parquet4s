package com.github.mjakubowski84.parquet4s

import org.apache.parquet.schema.{MessageType, Type}
import shapeless.labelled.FieldType
import shapeless.{::, HList, HNil, LabelledGeneric, Lazy, Witness}

import scala.reflect.ClassTag

/**
  * Type class that builds a schema of Parquet file for given type. Can skip some fields according to [[Cursor]]
  * indications.
  * @tparam T type for which schema is built
  */
trait SkippingParquetSchemaResolver[T] {

  /**
    * @param cursor facilitates traversal over T
    * @return list of [[org.apache.parquet.schema.Type]] for each product element that <i>T</i> contains.
    */
  def resolveSchema(cursor: Cursor): List[Type]

  /**
    * @return a name to be given to schema
    */
  def schemaName: Option[String] = None

}

object SkippingParquetSchemaResolver
  extends SchemaDefs {

  class TypedSchemaDefInvoker[K <: Symbol : Witness.Aux, V](schemaDef: TypedSchemaDef[V]) {
    private def fieldName = implicitly[Witness.Aux[K]].value.name
    def invoke(): Type = schemaDef(fieldName)
    def invokeOther(otherSchemaDef: TypedSchemaDef[V]): Type = otherSchemaDef(fieldName)
  }

  /**
    * Builds full Parquet file schema ([[org.apache.parquet.schema.MessageType]]) from <i>T</i>.
    * @param toSkip iterable of columns or dot-separated column paths that should be skipped when generating the schema
    */
  def resolveSchema[T](toSkip: Iterable[String])(implicit g: SkippingParquetSchemaResolver[T]): MessageType =
    Message(g.schemaName, g.resolveSchema(Cursor.skipping(toSkip)):_*)

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
      override def resolveSchema(cursor: Cursor): List[Type] = {
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
                             rest: Lazy[SkippingParquetSchemaResolver[G]],
                             classTag: ClassTag[T]
                            ): SkippingParquetSchemaResolver[T] = new SkippingParquetSchemaResolver[T] {
    override def resolveSchema(cursor: Cursor): List[Type] = rest.value.resolveSchema(cursor)
    override def schemaName: Option[String] = Option(classTag.runtimeClass.getCanonicalName)
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
