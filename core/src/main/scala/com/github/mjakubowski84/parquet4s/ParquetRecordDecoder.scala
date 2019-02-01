package com.github.mjakubowski84.parquet4s

import shapeless.labelled.{FieldType, field}
import shapeless.{::, HList, HNil, LabelledGeneric, Lazy, Witness}

import scala.language.higherKinds
import scala.util.control.NonFatal

/**
  * Type class that allows to decode instances of [[RowParquetRecord]]
  * @tparam T represents schema of [[RowParquetRecord]]
  */
trait ParquetRecordDecoder[T] {

  /**
    * @param record to be decoded to instance of given type
    * @return instance of product type decoded from record
    */
  def decode(record: RowParquetRecord): T

}

object ParquetRecordDecoder {

  object DecodingException {
    def apply(msg: String, cause: Throwable): DecodingException = {
      val decodingException = DecodingException(msg)
      decodingException.initCause(cause)
      decodingException
    }
  }

  case class DecodingException(msg: String) extends RuntimeException(msg)

  def apply[T](implicit ev: ParquetRecordDecoder[T]): ParquetRecordDecoder[T] = ev

  def decode[T](record: RowParquetRecord)(implicit ev: ParquetRecordDecoder[T]): T = ev.decode(record)

  implicit val nilDecoder: ParquetRecordDecoder[HNil] = new ParquetRecordDecoder[HNil] {
    override def decode(record: RowParquetRecord): HNil.type = HNil
  }

  implicit def headValueDecoder[FieldName <: Symbol, Head, Tail <: HList](implicit
                                                                          witness: Witness.Aux[FieldName],
                                                                          headDecoder: ValueCodec[Head],
                                                                          tailDecoder: ParquetRecordDecoder[Tail]
                                                                         ): ParquetRecordDecoder[FieldType[FieldName, Head] :: Tail] =
    new ParquetRecordDecoder[FieldType[FieldName, Head] :: Tail] {
      override def decode(record: RowParquetRecord): FieldType[FieldName, Head] :: Tail = {
        val fieldName = witness.value.name
        val fieldValue = record.fields.getOrElse(fieldName, NullValue)
        val decodedFieldValue = try {
          headDecoder.decode(fieldValue)
        } catch {
          case NonFatal(cause) =>
            throw DecodingException(s"Failed to decode field $fieldName of record: $record", cause)
        }
        field[FieldName](decodedFieldValue) :: tailDecoder.decode(record)
      }
    }

  implicit def genericDecoder[A, R](implicit
                                    gen: LabelledGeneric.Aux[A, R],
                                    decoder: Lazy[ParquetRecordDecoder[R]]
                                   ): ParquetRecordDecoder[A] =
    new ParquetRecordDecoder[A] {
      override def decode(record: RowParquetRecord): A = gen.from(decoder.value.decode(record))
    }

}
