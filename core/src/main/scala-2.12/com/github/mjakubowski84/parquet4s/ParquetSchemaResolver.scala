package com.github.mjakubowski84.parquet4s

import com.github.mjakubowski84.parquet4s.{TypedSchemaDef => TSD}
import org.apache.parquet.schema.*
import org.slf4j.LoggerFactory
import shapeless.*
import shapeless.labelled.*

import scala.annotation.implicitNotFound
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
trait ParquetSchemaResolver[T] {

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

}

object ParquetSchemaResolver {

  @deprecated("2.4.0", "use com.github.mjakubowski84.parquet4s.TypedSchemaDef directly instead")
  type TypedSchemaDef[V] = TSD[V]

  class TypedSchemaDefInvoker[V](val schema: TSD[V], fieldName: String) extends (() => Type) {
    override def apply(): Type = schema(fieldName)
  }

  trait SchemaVisitor[V] extends Cursor.Visitor[TypedSchemaDefInvoker[V], Option[Type]] {
    override def onCompleted(cursor: Cursor, invoker: TypedSchemaDefInvoker[V]): Option[Type] =
      throw new UnsupportedOperationException("Schema resolution cannot complete before all fields are processed.")
  }

  private val logger = LoggerFactory.getLogger(this.getClass)

  /** Builds full Parquet file schema ([[org.apache.parquet.schema.MessageType]]) from <i>T</i>.
    *
    * @param toSkip
    *   iterable of [[ColumnPath]]s that should be skipped when generating the schema
    */
  def resolveSchema[T](toSkip: Iterable[ColumnPath])(implicit g: ParquetSchemaResolver[T]): MessageType =
    Message(g.schemaName, g.resolveSchema(Cursor.skipping(toSkip))*)

  /** Builds full Parquet file schema ([[org.apache.parquet.schema.MessageType]]) from <i>T</i>.
    */
  def resolveSchema[T](implicit g: ParquetSchemaResolver[T]): MessageType =
    Message(g.schemaName, g.resolveSchema(Cursor.simple)*)

  implicit val hnil: ParquetSchemaResolver[HNil] = _ => List.empty

  implicit def hcons[K <: Symbol, V, T <: HList](implicit
      witness: Witness.Aux[K],
      typedSchemaDef: TSD[V],
      visitor: SchemaVisitor[V] = defaultSchemaVisitor[V],
      rest: ParquetSchemaResolver[T]
  ): ParquetSchemaResolver[FieldType[K, V] :: T] =
    cursor =>
      cursor
        .advance[K]
        .flatMap(newCursor =>
          newCursor.accept(new TypedSchemaDefInvoker(typedSchemaDef, witness.value.name), visitor)
        ) match {
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
    override def schemaName: Option[String] =
      Option(
        try classTag.runtimeClass.getCanonicalName
        catch {
          case e: Throwable =>
            logger
              .warn("Failed to resolve class name. Consider placing your class in static and less nested structure", e)
            null
        }
      )
  }

  def defaultSchemaVisitor[V]: SchemaVisitor[V] =
    (_, invoker: TypedSchemaDefInvoker[V]) => Option(invoker())

  /** Purpose of productSchemaVisitor is to filter product fields so that those that are used for partitioning are not
    * present in final schema. It is only applied to products that are not nested in Options and collections as optional
    * fields and elements of collections are not valid for partitioning.
    */
  implicit def productSchemaVisitor[V](implicit resolver: ParquetSchemaResolver[V]): SchemaVisitor[V] =
    (cursor: Cursor, invoker: TypedSchemaDefInvoker[V]) =>
      invoker.schema.wrapped match {
        // override fields only in generated groups (records), custom ones provided by users are not processed
        case _: GroupSchemaDef if invoker.schema.metadata.contains(SchemaDef.Meta.Generated) =>
          resolver.resolveSchema(cursor) match {
            case Nil =>
              None
            case fieldTypes =>
              Option(invoker().asGroupType().withNewFields(fieldTypes*))
          }
        case _ =>
          Option(invoker())
      }

}
