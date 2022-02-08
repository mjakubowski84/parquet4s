package com.github.mjakubowski84.parquet4s.compat

import com.github.mjakubowski84.parquet4s.Cursor

trait CursorCompat:

  this: Cursor =>

  /** @tparam FieldName
    *   String & Singleton of the field that cursor shall advance
    * @return
    *   a new cursor or None if advance to given field is disallowed
    */
  def advance[FieldName <: String & Singleton: ValueOf]: Option[Cursor] =
    advanceByFieldName(summon[ValueOf[FieldName]].value)

end CursorCompat
