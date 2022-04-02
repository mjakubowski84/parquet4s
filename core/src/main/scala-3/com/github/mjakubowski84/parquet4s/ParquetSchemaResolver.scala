package com.github.mjakubowski84.parquet4s

import com.github.mjakubowski84.parquet4s.{TypedSchemaDef => TSD}
import org.apache.parquet.schema.*
import org.slf4j.LoggerFactory

import scala.annotation.implicitNotFound
import scala.deriving.Mirror
import scala.language.higherKinds
import scala.reflect.ClassTag
import scala.util.NotGiven

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

object ParquetSchemaResolver:

  type TypedSchemaDef[V] = TSD[V]

  final abstract private[ParquetSchemaResolver] class Fields[Labels <: Tuple, Values <: Tuple]

  trait SchemaVisitor[V] extends Cursor.Visitor[String, Option[Type]]:
    override def onCompleted(cursor: Cursor, fieldName: String): Option[Type] =
      throw new UnsupportedOperationException("Schema resolution cannot complete before all fields are processed.")

  private val logger = LoggerFactory.getLogger(this.getClass)

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

  given [L <: String & Singleton: ValueOf, LT <: Tuple, V, VT <: Tuple](using
      visitor: SchemaVisitor[V],
      rest: ParquetSchemaResolver[Fields[LT, VT]]
  ): ParquetSchemaResolver[Fields[L *: LT, V *: VT]] with
    def resolveSchema(cursor: Cursor): List[Type] =
      cursor
        .advance[L]
        .flatMap(newCursor => newCursor.accept(summon[ValueOf[L]].value, visitor)) match
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
    override def schemaName: Option[String] =
      Option(
        try classTag.runtimeClass.getCanonicalName
        catch
          case e: Throwable =>
            logger
              .warn("Failed to resolve class name. Consider placing your class in static and less nested structure", e)
            null
      )

  given defaultSchemaVisitor[V: TSD](using NotGiven[ParquetSchemaResolver[V]]): SchemaVisitor[V] with
    def onActive(cursor: Cursor, fieldName: String): Option[Type] =
      Option(summon[TSD[V]](fieldName))

  /** Purpose of productSchemaVisitor is to filter product fields so that those that are used for partitioning are not
    * present in the final schema. It is only applied to products that are not nested in Options and collections - as
    * optional fields and elements of collections are not valid for partitioning.
    */
  given productSchemaVisitor[V <: Product: ParquetSchemaResolver: TSD]: SchemaVisitor[V] with
    def onActive(cursor: Cursor, fieldName: String): Option[Type] =
      summon[TSD[V]].wrapped match
        // override fields only in generated groups (records), custom ones provided by users are not processed
        case schema: GroupSchemaDef if schema.metadata.contains(SchemaDef.Meta.Generated) =>
          summon[ParquetSchemaResolver[V]].resolveSchema(cursor) match
            case Nil =>
              None
            case fieldTypes =>
              Option(schema(fieldName).asGroupType().withNewFields(fieldTypes*))
        case schema =>
          Option(schema(fieldName))

end ParquetSchemaResolver
