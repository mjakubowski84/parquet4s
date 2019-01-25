package com.github.mjakubowski84.parquet4s

import shapeless.labelled.{FieldType, field}
import shapeless.{::, HList, HNil, LabelledGeneric, Lazy, Witness}

import scala.language.higherKinds
import scala.util.control.NonFatal


trait ParquetRecordDecoder[T] {

  def decode(value: Value): T

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
    override def decode(record: Value): HNil.type = HNil
  }

  implicit def headValueDecoder[FieldName <: Symbol, Head, Tail <: HList](implicit
                                                                          witness: Witness.Aux[FieldName],
                                                                          headDecoder: ValueCodec[Head],
                                                                          tailDecoder: ParquetRecordDecoder[Tail]
                                                                         ): ParquetRecordDecoder[FieldType[FieldName, Head] :: Tail] =
    new ParquetRecordDecoder[FieldType[FieldName, Head] :: Tail] {
      override def decode(value: Value): FieldType[FieldName, Head] :: Tail = {
        value match {
          case record: RowParquetRecord =>
            val fieldName = witness.value.name
            val fieldValue = record.getMap.getOrElse(fieldName, NullValue)
            val decodedFieldValue = try {
              headDecoder.decode(fieldValue)
            } catch {
              case NonFatal(cause) =>
                throw DecodingException(s"Failed to decode field $fieldName of record: $record", cause)
            }
            field[FieldName](decodedFieldValue) :: tailDecoder.decode(record)
          case other =>
            throw DecodingException(s"Unexpected value type: $other. Expecting RowParquetRecord.")
        }
      }
    }

  implicit def genericDecoder[A, R](implicit
                                    gen: LabelledGeneric.Aux[A, R],
                                    decoder: Lazy[ParquetRecordDecoder[R]]
                                   ): ParquetRecordDecoder[A] =
    new ParquetRecordDecoder[A] {
      override def decode(record: Value): A = gen.from(decoder.value.decode(record))
    }

}
