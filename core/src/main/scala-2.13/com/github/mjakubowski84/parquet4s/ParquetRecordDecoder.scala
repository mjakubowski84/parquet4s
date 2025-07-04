package com.github.mjakubowski84.parquet4s

import shapeless.labelled.{FieldType, field}
import shapeless.{::, HList, HNil, LabelledGeneric, Lazy, Witness}

import scala.annotation.{implicitNotFound, unused}
import scala.util.control.NonFatal

/** Type class that allows to decode instances of [[RowParquetRecord]]
  * @tparam T
  *   represents schema of [[RowParquetRecord]]
  */
@implicitNotFound(
  "ParquetRecordDecoder. Cannot read data of type ${T}. " +
    "Please check if there is implicit ValueDecoder available for each field and subfield of ${T}."
)
trait ParquetRecordDecoder[T] extends MetadataReader {

  /** @param record
    *   to be decoded to instance of given type
    * @param configuration
    *   [ValueCodecConfiguration] used by some codecs
    * @return
    *   instance of product type decoded from record
    */
  def decode(record: RowParquetRecord, configuration: ValueCodecConfiguration): T

  override def setMetadata(@unused metadata: collection.Map[String, String]): Unit = {}
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

  def decode[T](record: RowParquetRecord, configuration: ValueCodecConfiguration = ValueCodecConfiguration.Default)(
      implicit ev: ParquetRecordDecoder[T]
  ): T = ev.decode(record, configuration)

  implicit val nilDecoder: ParquetRecordDecoder[HNil] = (_, _) => HNil

  implicit def headValueDecoder[FieldName <: Symbol, Head, Tail <: HList](implicit
      witness: Witness.Aux[FieldName],
      headDecoder: ValueDecoder[Head],
      tailDecoder: ParquetRecordDecoder[Tail]
  ): ParquetRecordDecoder[FieldType[FieldName, Head] :: Tail] =
    (record: RowParquetRecord, configuration: ValueCodecConfiguration) => {
      val fieldName = witness.value.name
      val decodedFieldValue =
        try headDecoder.decode(record.get(fieldName).getOrElse(NullValue), configuration)
        catch {
          case NonFatal(cause) =>
            throw DecodingException(s"Failed to decode field $fieldName of record: $record", cause)
        }

      field[FieldName](decodedFieldValue) :: tailDecoder.decode(record, configuration)
    }

  implicit def genericDecoder[A, R](implicit
      gen: LabelledGeneric.Aux[A, R],
      decoder: Lazy[ParquetRecordDecoder[R]]
  ): ParquetRecordDecoder[A] =
    (record: RowParquetRecord, configuration: ValueCodecConfiguration) =>
      gen.from(decoder.value.decode(record, configuration))

}
