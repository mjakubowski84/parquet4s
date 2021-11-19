package com.github.mjakubowski84.parquet4s

object Lookup {
  def apply(typedColumnPath: TypedColumnPath[?], ordinal: Int): Lookup =
    Lookup(typedColumnPath, ordinal, typedColumnPath.alias)
}

// TODO docs
case class Lookup(columnPath: ColumnPath, ordinal: Int, alias: Option[String]) {
  val length: Int = columnPath.elements.length
}
