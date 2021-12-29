package com.github.mjakubowski84.parquet4s

object ColumnProjection {
  def apply(typedColumnPath: TypedColumnPath[?], ordinal: Int): ColumnProjection =
    ColumnProjection(typedColumnPath, ordinal, typedColumnPath.alias)
}

/** Column projection extracts a value from a field at given path and sets it at given position, optionally with a new
  * name.
  * @param columnPath
  *   path to the field
  * @param ordinal
  *   position of a column in a schema defined by the projection
  * @param alias
  *   optional new name of the field
  */
case class ColumnProjection(columnPath: ColumnPath, ordinal: Int, alias: Option[String]) {
  val length: Int = columnPath.elements.length
}
