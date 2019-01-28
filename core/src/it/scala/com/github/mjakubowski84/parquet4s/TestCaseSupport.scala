package com.github.mjakubowski84.parquet4s

import com.github.mjakubowski84.parquet4s.CompatibilityParty.CompatibilityParty

import scala.reflect.runtime.universe.TypeTag

object CompatibilityParty {
  sealed trait CompatibilityParty
  case object Spark extends CompatibilityParty
  case object Reader extends CompatibilityParty
  case object Writer extends CompatibilityParty

  val All: Set[CompatibilityParty] = Set(Spark, Reader, Writer)
}

object Case {

  type CaseDef = Case[_ <: Product]

  def apply[T <: Product : TypeTag : ParquetReader : ParquetWriter](
                                                                     description: String,
                                                                     data: Seq[T],
                                                                     compatibilityParties: Set[CompatibilityParty] = CompatibilityParty.All
                                                                   ): Case[T] =
    new Case(
      description = description,
      compatibilityParties = compatibilityParties,
      _data = data,
      _reader = implicitly[ParquetReader[T]],
      _writer = implicitly[ParquetWriter[T]],
      _typeTag = implicitly[TypeTag[T]]
    )
}


class Case[T <: Product](
                          val description: String,
                          val compatibilityParties: Set[CompatibilityParty],
                          _data: Seq[T],
                          _reader: ParquetReader[T],
                          _writer: ParquetWriter[T],
                          _typeTag: TypeTag[T]
                        ) {
  type DataType = T
  def data: Seq[DataType] = _data
  def reader: ParquetReader[DataType] = _reader
  def writer: ParquetWriter[DataType] = _writer
  def typeTag: TypeTag[DataType] = _typeTag
}

trait TestCaseSupport {

  def caseDefinitions: Seq[Case.CaseDef]

  def cases(compatibilityParties: Set[CompatibilityParty] = CompatibilityParty.All): Seq[Case.CaseDef] =
    caseDefinitions.filter { caseDefinition =>
      compatibilityParties.forall(caseDefinition.compatibilityParties.contains)
    }

  def cases(compatibilityParty: CompatibilityParty*): Seq[Case.CaseDef] = cases(compatibilityParty.toSet)

  def singleCase[T: TypeTag]: Option[Case.CaseDef] = {
    val targetTag = implicitly[TypeTag[T]]
    caseDefinitions.find(_.typeTag.toString() == targetTag.toString())
  }

}
