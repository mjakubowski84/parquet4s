package com.github.mjakubowski84.parquet4s

object Lookup {
  def apply(typedColumnPath: TypedColumnPath[?], ordinal: Int): Lookup =
    Lookup(typedColumnPath, ordinal, typedColumnPath.alias)
}

/** Used in column projection to extract a value from a field at given path and set it at given position, optionally
  * with a new name.
  * @param columnPath
  *   path to the field
  * @param ordinal
  *   position of a column in a schema defined by the projection
  * @param alias
  *   optional new name of the field
  */
case class Lookup(columnPath: ColumnPath, ordinal: Int, alias: Option[String]) {
  val length: Int = columnPath.elements.length
}
