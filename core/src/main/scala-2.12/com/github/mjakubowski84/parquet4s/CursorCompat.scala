package com.github.mjakubowski84.parquet4s

import shapeless.Witness

trait CursorCompat {

  this: Cursor =>

  /** @tparam FieldName
    *   symbol of the field that cursor shall advance
    * @return
    *   a new cursor or None if advance to given field is disallowed
    */
  def advance[FieldName <: Symbol: Witness.Aux]: Option[Cursor] =
    advanceByFieldName(implicitly[Witness.Aux[FieldName]].value.name)

}
