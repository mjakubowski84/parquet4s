package com.github.mjakubowski84.parquet4s

import shapeless.labelled.FieldType
import shapeless.{::, HList, HNil, LabelledGeneric, Lazy, Witness}

private[parquet4s] trait StringLens[T] {

  def apply(from: T, path: List[String]): Either[StringLens.LensError, String]

}

private[parquet4s] object StringLens {

  case class LensError(tail: List[String], message: String)

  implicit val hnilStringLens: StringLens[HNil] = new StringLens[HNil] {
    override def apply(from: HNil, path: List[String]): Either[LensError, String] =
      Left(LensError(path, s"Field '${path.head}' does not exist."))
  }

  implicit def headValueLens[FieldName <: Symbol, Head, Tail <: HList](implicit
                                                                       witness: Witness.Aux[FieldName],
                                                                       headVisitor: FieldVisitor[Head] = defaultFieldVisitor[Head],
                                                                       tailLens: StringLens[Tail]
  ): StringLens[FieldType[FieldName, Head] :: Tail] =
    new StringLens[FieldType[FieldName, Head] :: Tail] {
      override def apply(from: FieldType[FieldName, Head] :: Tail,
                         path: List[String]): Either[LensError, String] = {
        val traversedFieldName = witness.value.name
        if (traversedFieldName == path.head) {
          headVisitor.visit(from.head, path.tail)
        } else
          tailLens.apply(from.tail, path)
      }
    }

  implicit def genericStringLens[T, R](implicit
                                       gen: LabelledGeneric.Aux[T, R],
                                       lens: Lazy[StringLens[R]]): StringLens[T] =
    new StringLens[T] {
      override def apply(from: T, path: List[String]): Either[LensError, String] =
        lens.value.apply(gen.to(from), path)
    }

  trait FieldVisitor[T] {
    def visit(field: T, path: List[String]): Either[LensError, String]
  }

  implicit def productFieldVisitor[T](implicit lens: StringLens[T]): FieldVisitor[T] = new FieldVisitor[T] {
    override def visit(field: T, path: List[String]): Either[LensError, String] = {
      if (path.isEmpty)
        Left(LensError(path, "Cannot partition by a Product class."))
      else
        lens.apply(field, path)
    }
  }

  implicit val stringFieldVisitor: FieldVisitor[String] =
    new FieldVisitor[String] {
      override def visit(field: String, path: List[String]): Either[LensError, String] = {
        if (path.nonEmpty)
          Left(LensError(path, s"Attempted to access field '${path.head}' from String."))
        else
          Right(field)
      }
    }

  private def defaultFieldVisitor[T]: FieldVisitor[T] =
    new FieldVisitor[T] {
      override def visit(field: T, path: List[String]): Either[LensError, String] =
        Left(LensError(path, "Only String field can be used for partitioning."))
    }

  def apply[T](obj: T, path: String)(implicit lens: StringLens[T]): String = {
    val pathList = path.split("\\.").toList
    lens.apply(obj, pathList) match {
      case Left(LensError(tail, message)) if tail.isEmpty =>
        throw new IllegalArgumentException(s"Invalid element at path '$path'. $message")
      case Left(LensError(tail, message)) if pathList.endsWith(tail) =>
        val errorPath = pathList.slice(0, pathList.lastIndexOfSlice(tail)).mkString(".")
        throw new IllegalArgumentException(s"Invalid element at path '$errorPath'. $message")
      case Left(LensError(_, message)) =>
        throw new IllegalArgumentException(s"Invalid path '$path'. $message")
      case Right(result) =>
        result
    }
  }

}
