package com.github.mjakubowski84.parquet4s

import java.util.NoSuchElementException

import com.github.mjakubowski84.parquet4s.CompatibilityParty.CompatibilityParty
import com.github.mjakubowski84.parquet4s.ParquetWriter.ParquetWriterFactory

object CompatibilityParty {
  sealed trait CompatibilityParty
  case object Spark extends CompatibilityParty
  case object Reader extends CompatibilityParty
  case object Writer extends CompatibilityParty

  val All: Set[CompatibilityParty] = Set(Spark, Reader, Writer)
}

object Case {

  type CaseDef = Case[_ <: Product]

  def apply[T <: Product : ParquetReader : ParquetWriterFactory](
                                                                            description: String,
                                                                            data: Seq[T],
                                                                            compatibilityParties: Set[CompatibilityParty] = CompatibilityParty.All
                                                                          ): Case[T] =
    new Case(
      description = description,
      compatibilityParties = compatibilityParties,
      _data = data,
      _reader = implicitly[ParquetReader[T]],
      _writerFactory = implicitly[ParquetWriterFactory[T]]
    )
}


class Case[T <: Product](
                          val description: String,
                          val compatibilityParties: Set[CompatibilityParty],
                          _data: Seq[T],
                          _reader: ParquetReader[T],
                          _writerFactory: ParquetWriterFactory[T]
                        ) {
  opaque type DataType = T
  def data: Seq[DataType] = _data
  def reader: ParquetReader[DataType] = _reader
  def writerFactory: ParquetWriterFactory[DataType] = _writerFactory
}

trait TestCaseSupport {

  def caseDefinitions: Seq[Case.CaseDef]

  def cases(compatibilityParties: Set[CompatibilityParty] = CompatibilityParty.All): Seq[Case.CaseDef] =
    caseDefinitions.filter { caseDefinition =>
      compatibilityParties.forall(caseDefinition.compatibilityParties.contains)
    }

  def cases(compatibilityParty: CompatibilityParty*): Seq[Case.CaseDef] = cases(compatibilityParty.toSet)

}
