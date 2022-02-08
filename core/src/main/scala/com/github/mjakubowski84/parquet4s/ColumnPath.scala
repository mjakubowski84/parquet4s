package com.github.mjakubowski84.parquet4s

import com.github.mjakubowski84.parquet4s.ParquetSchemaResolver.TypedSchemaDef
import org.apache.parquet.schema.Type

import scala.jdk.CollectionConverters.*

/** Short alias for [[ColumnPath]].
  */
object Col {

  /** Use dot notation to separate path elements.
    * @example
    *   {{{Col("user.address.postcode")}}}
    */
  def apply(column: String): ColumnPath = ColumnPath(column)
}

object ColumnPath {

  val Separator: Char = '.'

  /** Use dot notation to separate path elements.
    * @example
    *   {{{Col("user.address.postcode")}}}
    */
  def apply(path: String): ColumnPath =
    new ColumnPath(path.split(Separator).toList.filter(_.trim.nonEmpty))

  def apply(elements: Seq[String]): ColumnPath = new ColumnPath(elements)

  private[parquet4s] def apply(internal: org.apache.parquet.hadoop.metadata.ColumnPath): ColumnPath =
    this.apply(internal.asScala.toSeq)

  def unapply(columnPath: ColumnPath): Option[Product2[String, ColumnPath]] =
    columnPath.elements match {
      case Nil =>
        None
      case head :: Nil =>
        Some(head, Empty)
      case head :: tail =>
        Some(head, new ColumnPath(tail))
    }

  val Empty: ColumnPath = new ColumnPath(List.empty)

}

/** Represents a path leading through the tree of schema fields. Points a column in a schema of Parquet file.
  *
  * Can be used to define a filter.
  *
  * @example
  *   {{{Col("user.address.postcode") === "00000"}}}
  */
class ColumnPath protected (val elements: Seq[String]) extends FilterOps {
  override def toString: String = elements.mkString(ColumnPath.Separator.toString)

  def isEmpty: Boolean = elements.isEmpty

  def appendElement(element: String) = new ColumnPath(elements :+ element)

  def startsWith(other: ColumnPath): Boolean = this.elements.startsWith(other.elements)

  def canEqual(other: Any): Boolean = other.isInstanceOf[ColumnPath]

  def as[T: TypedSchemaDef]: TypedColumnPath[T] = TypedColumnPath(this)

  override def equals(other: Any): Boolean = other match {
    case that: ColumnPath =>
      (that canEqual this) &&
        elements == that.elements
    case _ => false
  }

  override def hashCode(): Int = elements.hashCode()
}

object TypedColumnPath {

  def apply[T: TypedSchemaDef](columnPath: ColumnPath): TypedColumnPath[T] = new TypedColumnPath[T](columnPath.elements)

}

class TypedColumnPath[T] private (elements: Seq[String], val alias: Option[String] = None)(implicit
    schema: TypedSchemaDef[T]
) extends ColumnPath(elements) {

  /** Turns the column to Parquet [[org.apache.parquet.schema.Type]]
    */
  def toType: Type = toType(elements, schema)

  private def toType(elements: Seq[String], leafSchema: TypedSchemaDef[T]): Type =
    elements match {
      case head :: Nil =>
        leafSchema.apply(head)
      case head :: tail =>
        SchemaDef.group(toType(tail, leafSchema)).apply(head)
    }

  /** Sets an alias for this column path. Alias changes the name of the column during reading.
    * @param alias
    *   a new name of the column
    * @return
    *   This column path with a new alias.
    */
  def alias(alias: String): TypedColumnPath[T] = new TypedColumnPath[T](elements, Some(alias))

}
