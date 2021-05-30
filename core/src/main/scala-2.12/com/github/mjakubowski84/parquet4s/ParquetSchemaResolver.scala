package com.github.mjakubowski84.parquet4s

import com.github.mjakubowski84.parquet4s.ParquetSchemaResolver.TypedSchemaDef
import org.apache.parquet.schema.LogicalTypeAnnotation.{DateLogicalTypeAnnotation, DecimalLogicalTypeAnnotation, IntLogicalTypeAnnotation, StringLogicalTypeAnnotation}
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName._
import org.apache.parquet.schema.Type.Repetition
import org.apache.parquet.schema._
import shapeless._
import shapeless.labelled._

import scala.annotation.implicitNotFound
import scala.language.higherKinds
import scala.reflect.ClassTag

/**
  * Type class that allows to build schema of Parquet file out from regular Scala type, typically case class.
  * @tparam T scala type that represents schema of Parquet data.
  */
@implicitNotFound("Cannot write data of type ${T}. " +
  "Please check if there is implicit TypedSchemaDef available for each field and subfield of ${T}."
)
trait ParquetSchemaResolver[T] {

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

object ParquetSchemaResolver
  extends SchemaDefs {

  trait Tag[V]
  type TypedSchemaDef[V] = SchemaDef with Tag[V]

  class TypedSchemaDefInvoker[K <: Symbol : Witness.Aux, V](schemaDef: TypedSchemaDef[V]) {
    private def fieldName = implicitly[Witness.Aux[K]].value.name
    def `type`: Type = schemaDef(fieldName)
    def productType(productSchemaDef: TypedSchemaDef[V]): Type = productSchemaDef(fieldName)
  }

  /**
   * Builds full Parquet file schema ([[org.apache.parquet.schema.MessageType]]) from <i>T</i>.
   *
   * @param toSkip iterable of [[ColumnPath]]s that should be skipped when generating the schema
   */
  def resolveSchema[T](toSkip: Iterable[ColumnPath])(implicit g: ParquetSchemaResolver[T]): MessageType =
    Message(g.schemaName, g.resolveSchema(Cursor.skipping(toSkip)):_*)

  /**
   * Builds full Parquet file schema ([[org.apache.parquet.schema.MessageType]]) from <i>T</i>.
   */
  def resolveSchema[T](implicit g: ParquetSchemaResolver[T]): MessageType =
    Message(g.schemaName, g.resolveSchema(Cursor.simple):_*)

  implicit val hnil: ParquetSchemaResolver[HNil] = _ => List.empty

  implicit def hcons[K <: Symbol, V, T <: HList](implicit
                                                 witness: Witness.Aux[K],
                                                 schemaDef: TypedSchemaDef[V],
                                                 visitor: SchemaVisitor[K, V] = defaultSchemaVisitor[K, V],
                                                 rest: ParquetSchemaResolver[T]
                                                ): ParquetSchemaResolver[FieldType[K, V] :: T] =
    cursor => cursor
      .advance[K]
      .flatMap(newCursor => newCursor.accept(new TypedSchemaDefInvoker(schemaDef), visitor)) match {
        case Some(head) =>
          head +: rest.resolveSchema(cursor)
        case None =>
          rest.resolveSchema(cursor)
      }

  implicit def generic[T, G](implicit
                             lg: LabelledGeneric.Aux[T, G],
                             rest: Lazy[ParquetSchemaResolver[G]],
                             classTag: ClassTag[T]
                            ): ParquetSchemaResolver[T] = new ParquetSchemaResolver[T] {
    override def resolveSchema(cursor: Cursor): List[Type] = rest.value.resolveSchema(cursor)
    override def schemaName: Option[String] = Option(classTag.runtimeClass.getCanonicalName)
  }

  trait SchemaVisitor[K <: Symbol, V] extends Cursor.Visitor[TypedSchemaDefInvoker[K, V], Option[Type]] {
    override def onCompleted(cursor: Cursor, invoker: TypedSchemaDefInvoker[K, V]): Option[Type] =
      throw new UnsupportedOperationException("Schema resolution cannot complete before all fields are processed.")
  }

  def defaultSchemaVisitor[K <: Symbol, V]: SchemaVisitor[K, V] =
    (_, invoker: TypedSchemaDefInvoker[K, V]) => Option(invoker.`type`)

  implicit def productSchemaVisitor[K <: Symbol, V](implicit resolver: ParquetSchemaResolver[V]): SchemaVisitor[K, V] =
    (cursor: Cursor, invoker: TypedSchemaDefInvoker[K, V]) => resolver.resolveSchema(cursor) match {
      case Nil =>
        None
      case fieldTypes =>
        Option(invoker.productType(SchemaDef.group(fieldTypes: _*).typed[V]))
    }


}
