package com.github.mjakubowski84.parquet4s

import shapeless.Witness

/**
  * Auxiliary construct that facilitates traversal over tree of case classes.
  * Apart from pointing if program should advance the tree it implements visitor pattern that allows to process
  * the content of the tree in context of cursor's state.
  */
trait Cursor {

  /**
    * @return current path of the cursor
    */
  def path: ColumnPath

  /**
    * @tparam FieldName symbol of the field that cursor shall advance
    * @return a new cursor or None if advance to given field is disallowed
    */
  def advance[FieldName <: Symbol: Witness.Aux]: Option[Cursor]

  /**
    * Processes given object in context of cursor's state
    * @param obj object to process
    * @param visitor visitor that shall process the the object
    * @tparam T type of the object
    * @tparam R type of processing result
    * @return result of the processing
    */
  def accept[T, R](obj: T, visitor: Cursor.Visitor[T, R]): R

}

object Cursor {

  /**
    * Creates an instance of [[Cursor]] that indicates which fields of case class tree should be skipped by the program.
    * @param toSkip iterable of columns/fields that shall be skipped
    * @return a new instance of the cursor
    */
  def skipping(toSkip: Iterable[ColumnPath]): Cursor = SkippingCursor(toSkip.toSet)

  /**
    * Creates an instance of [[Cursor]] that guides the program across the tree of case classes. Indicates if the
    * program shall advance given field and when shall stop.
    * @param path column path that the program shall follow
    * @return a new instance of the cursor
    */
  def following(path: ColumnPath): Cursor = FollowingCursor(path)

  /**
   * Creates an instance of [[Cursor]] that traverses case class without application of any specific logic.
   */
  def simple: Cursor = SimpleCursor()

  final class Completed(val path: ColumnPath) extends Cursor {
    override def accept[T, R](obj: T, visitor: Visitor[T, R]): R = visitor.onCompleted(this, obj)
    override def advance[FieldName <: Symbol : Witness.Aux]: Option[Cursor] = None
  }

  trait Active {
    this: Cursor =>
    override def accept[T, R](obj: T, visitor: Visitor[T, R]): R = visitor.onActive(this, obj)
  }

  /**
    * Processes an object in a context of the cursor.
    * @tparam T type of the object to process
    * @tparam R type of the result of processing
    */
  trait Visitor[T, R] {
    /**
      * To be called when the cursor is in completed state.
      */
    def onCompleted(cursor: Cursor, obj: T): R
    /**
      * To be called when the cursor is in active state.
      */
    def onActive(cursor: Cursor, obj: T): R
  }

}

private object SkippingCursor {
  def apply(toSkip: Set[ColumnPath]): SkippingCursor = new SkippingCursor(ColumnPath.Empty, toSkip)
}

private class SkippingCursor private (val path: ColumnPath, toSkip: Set[ColumnPath]) extends Cursor with Cursor.Active {

  override def advance[FieldName <: Symbol: Witness.Aux]: Option[Cursor] = {
    val newPath = path.appendElement(implicitly[Witness.Aux[FieldName]].value.name)
    if (toSkip.contains(newPath)) None
    else Some(new SkippingCursor(newPath, toSkip))
  }

}

private object FollowingCursor {
  def apply(toFollow: ColumnPath): Cursor =
    if (toFollow.isEmpty) new Cursor.Completed(toFollow)
    else new FollowingCursor(ColumnPath.Empty, toFollow)
}

private class FollowingCursor private(val path: ColumnPath, toFollow: ColumnPath) extends Cursor with Cursor.Active {

  override def advance[FieldName <: Symbol: Witness.Aux]: Option[Cursor] = {
    val newPath = path.appendElement(implicitly[Witness.Aux[FieldName]].value.name)
    if (toFollow == newPath)
      Some(new Cursor.Completed(newPath))
    else if (toFollow.startsWith(newPath))
      Some(new FollowingCursor(newPath, toFollow))
    else None
  }

}

private object SimpleCursor {
  def apply(): Cursor = new SimpleCursor(ColumnPath.Empty)
}

private class SimpleCursor private(val path: ColumnPath) extends Cursor with Cursor.Active {

  override def advance[FieldName <: Symbol: Witness.Aux]: Option[Cursor] = {
    val newPath = path.appendElement(implicitly[Witness.Aux[FieldName]].value.name)
    Some(new SimpleCursor(newPath))
  }

}
