package com.github.mjakubowski84.parquet4s

import com.github.mjakubowski84.parquet4s.{TypedSchemaDef => TSD}
import org.apache.parquet.schema.*
import org.slf4j.LoggerFactory

import scala.annotation.implicitNotFound
import scala.deriving.Mirror
import scala.jdk.CollectionConverters.*
import scala.language.higherKinds
import scala.reflect.ClassTag
import scala.util.NotGiven
import scala.util.Try

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

  @deprecated("2.4.0", "use com.github.mjakubowski84.parquet4s.TypedSchemaDef directly instead")
  type TypedSchemaDef[V] = TSD[V]

  final abstract private[ParquetSchemaResolver] class Fields[Labels <: Tuple, Values <: Tuple]

  trait SchemaVisitor[V] extends Cursor.Visitor[String, Option[Type]]

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

  /** Finds a type at the given path
    */
  def findType[T](columnPath: ColumnPath)(using g: ParquetSchemaResolver[T]): Option[Type] =
    leafType(
      g.resolveSchema(Cursor.following(columnPath)).headOption,
      if (columnPath.isEmpty) Seq.empty else columnPath.elements.tail
    )

  private def leafType(typeOpt: Option[Type], pathElements: Seq[String]): Option[Type] =
    typeOpt match {
      case Some(group: GroupType) if pathElements.nonEmpty =>
        leafType(Try(group.getType(pathElements.head)).toOption, pathElements.tail)
      case _ =>
        typeOpt
    }

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

    def onCompleted(cursor: Cursor, fieldName: String): Option[Type] =
      Option(summon[TSD[V]](fieldName))

  /** Purpose of productSchemaVisitor is to filter product fields so that those that are used for partitioning are not
    * present in the final schema. It is only applied to products that are not nested in Options and collections - as
    * optional fields and elements of collections are not valid for partitioning.
    */
  given productSchemaVisitor[V <: Product: TSD]: SchemaVisitor[V] with
    def onActive(cursor: Cursor, fieldName: String): Option[Type] =
      summon[TSD[V]] match
        case schema if schema.isGroup =>
          val groupType = schema(fieldName).asGroupType()
          applyCursor(cursor, groupType) match {
            case Nil =>
              None
            case filteredFields =>
              Option(groupType.withNewFields(filteredFields*))
          }
        case schema =>
          Option(schema(fieldName))

    def onCompleted(cursor: Cursor, fieldName: String): Option[Type] =
      Option(summon[TSD[V]](fieldName))

  private[parquet4s] def applyCursor(cursor: Cursor, group: GroupType): List[Type] =
    val fields = group.getFields.asScala.toList
    fields.flatMap {
      case groupField: GroupType if groupField.getLogicalTypeAnnotation == null =>
        cursor.advanceByFieldName(groupField.getName).flatMap { newCursor =>
          val fields = applyCursor(newCursor, groupField)
          if (fields.isEmpty) None
          else
            Some(
              SchemaDef
                .group(fields*)
                .withRequired(groupField.getRepetition == Type.Repetition.REQUIRED)(groupField.getName)
            )
        }
      case field =>
        cursor.advanceByFieldName(field.getName).map(_ => field)
    }

end ParquetSchemaResolver
