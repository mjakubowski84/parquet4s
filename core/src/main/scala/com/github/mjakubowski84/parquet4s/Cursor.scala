package com.github.mjakubowski84.parquet4s

import shapeless.Witness

import java.util.Calendar

/** Auxiliary construct that facilitates traversal over tree of case classes. Apart from pointing if program should
  * advance the tree it implements visitor pattern that allows to process the content of the tree in context of cursor's
  * state.
  */
trait Cursor {

  /** @return
    *   current path of the cursor
    */
  def path: Cursor.DotPath

  /** @tparam FieldName
    *   symbol of the field that cursor shall advance
    * @return
    *   a new cursor or None if advance to given field is disallowed
    */
  def advance[FieldName <: Symbol: Witness.Aux]: Option[Cursor]

  /** Processes given object in context of cursor's state
    * @param obj
    *   object to process
    * @param visitor
    *   visitor that shall process the the object
    * @tparam T
    *   type of the object
    * @tparam R
    *   type of processing result
    * @return
    *   result of the processing
    */
  def accept[T, R](obj: T, visitor: Cursor.Visitor[T, R]): R

  /** @return
    *   Text description what the cursor seeks for
    */
  def objectiveAsString: String

  /** @return
    *   what the cursor seeks for
    */
  def objective: Any

}

object Cursor {

  Calendar.getInstance.getTime

  type DotPath = List[String]
  object DotPath {
    val empty: DotPath                         = List.empty
    def apply(str: String): DotPath            = str.split("\\.").toList
    def unapply(path: DotPath): Option[String] = Some(toString(path))
    def toString(path: DotPath): String        = path.mkString(".")
  }

  /** Creates an instance of [[Cursor]] that indicates which fields of case class tree should be skipped by the program
    * @param toSkip
    *   iterable of columns/fields that shall be skipped
    * @return
    *   a new instance of the cursor
    */
  def skipping(toSkip: Iterable[String]): Cursor = SkippingCursor(toSkip.map(DotPath.apply).toSet)

  /** Creates an instance of [[Cursor]] that guides the program across the tree of case classes. Indicates if the
    * program shall advance given field and when shall stop.
    * @param path
    *   column path that the program shall follow
    * @return
    *   a new instance of the cursor
    */
  def following(path: String): Cursor = FollowingCursor(DotPath(path))

  final class Completed(val path: DotPath, val objective: Any, val objectiveAsString: String) extends Cursor {
    override def accept[T, R](obj: T, visitor: Visitor[T, R]): R           = visitor.onCompleted(this, obj)
    override def advance[FieldName <: Symbol: Witness.Aux]: Option[Cursor] = None
  }

  trait Active {
    this: Cursor =>
    override def accept[T, R](obj: T, visitor: Visitor[T, R]): R = visitor.onActive(this, obj)
  }

  /** Processes an object in a context of the cursor.
    * @tparam T
    *   type of the object to process
    * @tparam R
    *   type of the result of processing
    */
  trait Visitor[T, R] {

    /** To be called when the cursor is in completed state.
      */
    def onCompleted(cursor: Cursor, obj: T): R

    /** To be called when the cursor is in active state.
      */
    def onActive(cursor: Cursor, obj: T): R
  }

}

private object SkippingCursor {
  def apply(toSkip: Set[Cursor.DotPath]): SkippingCursor = new SkippingCursor(Cursor.DotPath.empty, toSkip)
}

private class SkippingCursor private (val path: Cursor.DotPath, toSkip: Set[Cursor.DotPath])
    extends Cursor
    with Cursor.Active {

  override lazy val objective: Any            = toSkip
  override lazy val objectiveAsString: String = toSkip.map(Cursor.DotPath.toString).mkString("[", ", ", "]")

  override def advance[FieldName <: Symbol: Witness.Aux]: Option[Cursor] = {
    val newPath = path :+ implicitly[Witness.Aux[FieldName]].value.name
    if (toSkip.contains(newPath)) None
    else Some(new SkippingCursor(newPath, toSkip))
  }

}

private object FollowingCursor {
  def apply(toFollow: Cursor.DotPath): Cursor =
    if (toFollow.isEmpty) new Cursor.Completed(Cursor.DotPath.empty, objective = toFollow, objectiveAsString = "")
    else new FollowingCursor(Cursor.DotPath.empty, toFollow)
}

private class FollowingCursor private (val path: Cursor.DotPath, toFollow: Cursor.DotPath)
    extends Cursor
    with Cursor.Active {

  override lazy val objective: Any            = toFollow
  override lazy val objectiveAsString: String = Cursor.DotPath.toString(toFollow)

  override def advance[FieldName <: Symbol: Witness.Aux]: Option[Cursor] = {
    val newPath = path :+ implicitly[Witness.Aux[FieldName]].value.name
    if (toFollow == newPath)
      Some(new Cursor.Completed(newPath, objective, objectiveAsString))
    else if (toFollow.startsWith(newPath))
      Some(new FollowingCursor(newPath, toFollow))
    else None
  }

}
