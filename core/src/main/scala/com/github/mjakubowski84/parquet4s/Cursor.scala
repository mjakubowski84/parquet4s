package com.github.mjakubowski84.parquet4s

import shapeless.Witness

trait Cursor {

  def path: Cursor.DotPath

  def advance[FieldName <: Symbol: Witness.Aux]: Option[Cursor]

  def accept[T, R](obj: T, visitor: Cursor.Visitor[T, R]): R

  def objective: String

}

object Cursor {

  type DotPath = List[String]
  object DotPath {
    val empty: DotPath = List.empty
    def apply(str: String): DotPath = str.split("\\.").toList
    def unapply(path: DotPath): Option[String] = Some(toString(path))
    def toString(path: DotPath): String = path.mkString(".")
  }

  def skipping(toSkip: Iterable[String]): Cursor = SkippingCursor(toSkip.map(DotPath.apply).toSet)

  def following(path: String): Cursor = FollowingCursor(DotPath(path))

  final class Completed(val path: DotPath, val objective: String) extends Cursor {
    override def accept[T, R](obj: T, visitor: Visitor[T, R]): R = visitor.onCompleted(this, obj)
    override def advance[FieldName <: Symbol : Witness.Aux]: Option[Cursor] = None
  }

  trait Active {
    this: Cursor =>
    override def accept[T, R](obj: T, visitor: Visitor[T, R]): R = visitor.onActive(this, obj)
  }

  trait Visitor[T, R] {
    def onCompleted(cursor: Cursor, obj: T): R
    def onActive(cursor: Cursor, obj: T): R
  }

}

private object SkippingCursor {
  def apply(toSkip: Set[Cursor.DotPath]): SkippingCursor = new SkippingCursor(Cursor.DotPath.empty, toSkip)
}

private class SkippingCursor private (val path: Cursor.DotPath, toSkip: Set[Cursor.DotPath]) extends Cursor with Cursor.Active {

  override lazy val objective: String = toSkip.map(Cursor.DotPath.toString).mkString("[", ", ", "]")

  override def advance[FieldName <: Symbol: Witness.Aux]: Option[Cursor] = {
    val newPath = path :+ implicitly[Witness.Aux[FieldName]].value.name
    if (toSkip.contains(newPath)) None
    else Some(new SkippingCursor(newPath, toSkip))
  }

}

private object FollowingCursor {
  def apply(toFollow: Cursor.DotPath): Cursor =
    if (toFollow.isEmpty) new Cursor.Completed(Cursor.DotPath.empty, "")
    else new FollowingCursor(Cursor.DotPath.empty, toFollow)
}

private class FollowingCursor private(val path: Cursor.DotPath, toFollow: Cursor.DotPath) extends Cursor with Cursor.Active {

  override lazy val objective: String = Cursor.DotPath.toString(toFollow)

  override def advance[FieldName <: Symbol: Witness.Aux]: Option[Cursor] = {
    val newPath = path :+ implicitly[Witness.Aux[FieldName]].value.name
    if (toFollow == newPath)
      Some(new Cursor.Completed(newPath, objective))
    else if (toFollow.startsWith(newPath))
      Some(new FollowingCursor(newPath, toFollow))
    else None
  }

}
