package com.github.mjakubowski84.parquet4s

import com.github.mjakubowski84.parquet4s.ParquetSchemaResolver.TypedSchemaDef
import org.apache.parquet.schema.LogicalTypeAnnotation.{
  DateLogicalTypeAnnotation,
  DecimalLogicalTypeAnnotation,
  IntLogicalTypeAnnotation,
  StringLogicalTypeAnnotation
}
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName._
import org.apache.parquet.schema.Type.Repetition
import org.apache.parquet.schema._

import scala.annotation.implicitNotFound
import scala.deriving.Mirror
import scala.language.higherKinds
import scala.reflect.ClassTag

/** Type class that allows to build schema of Parquet file out from regular Scala type, typically case class.
  * @tparam T
  *   scala type that represents schema of Parquet data.
  */
@implicitNotFound(
  "Cannot write data of type ${T}. " +
    "Please check if there is implicit TypedSchemaDef available for each field and subfield of ${T}."
)
trait ParquetSchemaResolver[T]:
  /** @param cursor
    *   facilitates traversal over T
    * @return
    *   list of [[org.apache.parquet.schema.Type]] for each product element that <i>T</i> contains.
    */
  def resolveSchema(cursor: Cursor): List[Type]

  /** @return
    *   a name to be given to schema
    */
  def schemaName: Option[String] = None

object ParquetSchemaResolver extends SchemaDefs:

  final abstract class Tag[V]
  opaque type Tagged[+T] = Any
  type TypedSchemaDef[V] = SchemaDef & Tagged[Tag[V]]

  final abstract private[ParquetSchemaResolver] class Fields[Labels <: Tuple, Values <: Tuple]

  class TypedSchemaDefInvoker[L <: String & Singleton: ValueOf, V](schemaDef: TypedSchemaDef[V]):
    private def fieldName                                      = summon[ValueOf[L]].value
    def `type`: Type                                           = schemaDef(fieldName)
    def productType(productSchemaDef: TypedSchemaDef[V]): Type = productSchemaDef(fieldName)

  /** Builds full Parquet file schema ([[org.apache.parquet.schema.MessageType]]) from <i>T</i>.
    *
    * @param toSkip
    *   iterable of [[ColumnPath]]s that should be skipped when generating the schema
    */
  def resolveSchema[T](toSkip: Iterable[ColumnPath])(using g: ParquetSchemaResolver[T]): MessageType =
    Message(g.schemaName, g.resolveSchema(Cursor.skipping(toSkip)): _*)

  /** Builds full Parquet file schema ([[org.apache.parquet.schema.MessageType]]) from <i>T</i>.
    */
  def resolveSchema[T](using g: ParquetSchemaResolver[T]): MessageType =
    Message(g.schemaName, g.resolveSchema(Cursor.simple): _*)

  given ParquetSchemaResolver[Fields[EmptyTuple, EmptyTuple]] with
    def resolveSchema(cursor: Cursor): List[Type] = List.empty

  given [L <: String & Singleton: ValueOf, LT <: Tuple, V: TypedSchemaDef, VT <: Tuple](using
      visitor: SchemaVisitor[L, V],
      rest: ParquetSchemaResolver[Fields[LT, VT]]
  ): ParquetSchemaResolver[Fields[L *: LT, V *: VT]] with
    def resolveSchema(cursor: Cursor): List[Type] =
      cursor
        .advance[L]
        .flatMap(newCursor =>
          newCursor.accept(new TypedSchemaDefInvoker[L, V](summon[TypedSchemaDef[V]]), visitor)
        ) match
        case Some(head) =>
          head +: rest.resolveSchema(cursor)
        case None =>
          rest.resolveSchema(cursor)

  given derived[P <: Product](using
      mirror: Mirror.ProductOf[P],
      rest: ParquetSchemaResolver[Fields[mirror.MirroredElemLabels, mirror.MirroredElemTypes]],
      classTag: ClassTag[P]
  ): ParquetSchemaResolver[P] with
    def resolveSchema(cursor: Cursor): List[Type] = rest.resolveSchema(cursor)
    override def schemaName: Option[String]       = Option(classTag.runtimeClass.getCanonicalName)

  trait SchemaVisitor[L <: String & Singleton, V] extends Cursor.Visitor[TypedSchemaDefInvoker[L, V], Option[Type]]:
    override def onCompleted(cursor: Cursor, invoker: TypedSchemaDefInvoker[L, V]): Option[Type] =
      throw new UnsupportedOperationException("Schema resolution cannot complete before all fields are processed.")

  given defaultSchemaVisitor[L <: String & Singleton, V]: SchemaVisitor[L, V] with
    def onActive(cursor: Cursor, invoker: TypedSchemaDefInvoker[L, V]): Option[Type] = Option(invoker.`type`)

  given [L <: String & Singleton, V <: Product: ParquetSchemaResolver]: SchemaVisitor[L, V] with
    def onActive(cursor: Cursor, invoker: TypedSchemaDefInvoker[L, V]): Option[Type] =
      summon[ParquetSchemaResolver[V]].resolveSchema(cursor) match
        case Nil =>
          None
        case fieldTypes =>
          Option(invoker.productType(SchemaDef.group(fieldTypes: _*).typed[V]))

end ParquetSchemaResolver
