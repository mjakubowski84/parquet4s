package com.github.mjakubowski84.parquet4s

import com.github.mjakubowski84.parquet4s.CompatibilityParty.CompatibilityParty

import java.util.NoSuchElementException

object CompatibilityParty {
  sealed trait CompatibilityParty
  case object Spark extends CompatibilityParty
  case object Reader extends CompatibilityParty
  case object Writer extends CompatibilityParty

  val All: Set[CompatibilityParty] = Set(Spark, Reader, Writer)
}

object Case {

  type CaseDef = Case[? <: Product]

  def apply[T <: Product: ParquetRecordDecoder: ParquetRecordEncoder: ParquetSchemaResolver](description: String,
                                                                                             data: Seq[T],
                                                                                             compatibilityParties: Set[CompatibilityParty] = CompatibilityParty.All
                                                                                            ): Case[T] =
    new Case(
      description = description,
      compatibilityParties = compatibilityParties,
      _data = data,
      _decoder = implicitly[ParquetRecordDecoder[T]],
      _encoder = implicitly[ParquetRecordEncoder[T]],
      _resolver = implicitly[ParquetSchemaResolver[T]]
    )
}


class Case[T <: Product](
                          val description: String,
                          val compatibilityParties: Set[CompatibilityParty],
                          _data: Seq[T],
                          _decoder: ParquetRecordDecoder[T],
                          _encoder: ParquetRecordEncoder[T],
                          _resolver: ParquetSchemaResolver[T]
                        ) {
  opaque type DataType = T
  def data: Seq[DataType] = _data
  def decoder: ParquetRecordDecoder[DataType] = _decoder
  def encoder: ParquetRecordEncoder[DataType] = _encoder
  def resolver: ParquetSchemaResolver[DataType] = _resolver
}

trait TestCaseSupport {

  def caseDefinitions: Seq[Case.CaseDef]

  def cases(compatibilityParties: Set[CompatibilityParty] = CompatibilityParty.All): Seq[Case.CaseDef] =
    caseDefinitions.filter { caseDefinition =>
      compatibilityParties.forall(caseDefinition.compatibilityParties.contains)
    }

  def cases(compatibilityParty: CompatibilityParty*): Seq[Case.CaseDef] = cases(compatibilityParty.toSet)

}
