package com.github.mjakubowski84.parquet4s

import shapeless.labelled.FieldType
import shapeless.{::, HList, HNil, LabelledGeneric, Lazy, Witness}

import scala.language.higherKinds
import scala.util.control.NonFatal


/**
  * Type class that allows to encode given data entity as [[RowParquetRecord]]
  * @tparam T type of source data
  */
trait ParquetRecordEncoder[T] {

  /**
    * @param entity data to be encoded
    * @param configuration [ValueCodecConfiguration] used by some codecs
    * @return [[RowParquetRecord]] containing product elements from the data
    */
  def encode(entity: T, configuration: ValueCodecConfiguration): RowParquetRecord

}

object ParquetRecordEncoder {

  object EncodingException {
    def apply(msg: String, cause: Throwable): EncodingException = {
      val encodingException = EncodingException(msg)
      encodingException.initCause(cause)
      encodingException
    }
  }

  case class EncodingException(msg: String) extends RuntimeException(msg)

  def apply[T](implicit ev: ParquetRecordEncoder[T]): ParquetRecordEncoder[T] = ev

  def encode[T](entity: T, configuration: ValueCodecConfiguration = ValueCodecConfiguration.default)
               (implicit ev: ParquetRecordEncoder[T]): RowParquetRecord = ev.encode(entity, configuration)

  implicit val nilEncoder: ParquetRecordEncoder[HNil] = (_, _) => RowParquetRecord.EmptyNoSchema

  implicit def headValueEncoder[FieldName <: Symbol, Head, Tail <: HList](implicit
                                                                          witness: Witness.Aux[FieldName],
                                                                          headEncoder: ValueCodec[Head],
                                                                          tailEncoder: ParquetRecordEncoder[Tail]
                                                                         ): ParquetRecordEncoder[FieldType[FieldName, Head] :: Tail] =
    (entity: FieldType[FieldName, Head] :: Tail, configuration: ValueCodecConfiguration) => {
      val fieldName = witness.value.name
      val fieldValue = try {
        headEncoder.encode(entity.head, configuration)
      } catch {
        case NonFatal(cause) =>
          throw EncodingException(s"Failed to encode field $fieldName: ${entity.head}, due to ${cause.getMessage}", cause)
      }
      tailEncoder.encode(entity.tail, configuration).prepended(fieldName, fieldValue)
    }

  implicit def genericEncoder[A, R](implicit
                                    gen: LabelledGeneric.Aux[A, R],
                                    encoder: Lazy[ParquetRecordEncoder[R]]
                                   ): ParquetRecordEncoder[A] =
    (entity: A, configuration: ValueCodecConfiguration) => encoder.value.encode(gen.to(entity), configuration)

}
