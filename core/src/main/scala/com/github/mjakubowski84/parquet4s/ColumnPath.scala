package com.github.mjakubowski84.parquet4s

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
class ColumnPath private (val elements: Seq[String]) extends FilterOps {
  override def toString: String = elements.mkString(ColumnPath.Separator.toString)

  def isEmpty: Boolean = elements.isEmpty

  def appendElement(element: String) = new ColumnPath(elements :+ element)

  def startsWith(other: ColumnPath): Boolean = this.elements.startsWith(other.elements)

  def canEqual(other: Any): Boolean = other.isInstanceOf[ColumnPath]

  override def equals(other: Any): Boolean = other match {
    case that: ColumnPath =>
      (that canEqual this) &&
        elements == that.elements
    case _ => false
  }

  override def hashCode(): Int = elements.hashCode()
}
