package com.github.mjakubowski84.parquet4s

import scala.deriving.Mirror

import scala.annotation.implicitNotFound
import scala.util.control.NonFatal

/**
  * Type class that allows to decode instances of [[RowParquetRecord]]
  * @tparam T represents schema of [[RowParquetRecord]]
  */
@implicitNotFound("ParquetRecordDecoder. Cannot read data of type ${T}. " +
  "Please check if there is implicit ValueDecoder available for each field and subfield of ${T}."
)
trait ParquetRecordDecoder[T]:
  /**
    * @param record to be decoded to instance of given type
    * @param configuration [ValueCodecConfiguration] used by some codecs
    * @return instance of product type decoded from record
    */
  def decode(record: RowParquetRecord, configuration: ValueCodecConfiguration): T

object ParquetRecordDecoder:

  object DecodingException:
    def apply(msg: String, cause: Throwable): DecodingException =
      val decodingException = DecodingException(msg)
      decodingException.initCause(cause)
      decodingException

  case class DecodingException(msg: String) extends RuntimeException(msg)

  private[ParquetRecordDecoder] class Fields[Labels <: Tuple, Values <: Tuple](val values: Values)

  def apply[T](using ev: ParquetRecordDecoder[T]): ParquetRecordDecoder[T] = ev

  def decode[T](record: RowParquetRecord, configuration: ValueCodecConfiguration = ValueCodecConfiguration.Default)
               (using ev: ParquetRecordDecoder[T]): T = ev.decode(record, configuration)

  given ParquetRecordDecoder[Fields[EmptyTuple, EmptyTuple]] with
    def decode(record: RowParquetRecord, configuration: ValueCodecConfiguration): Fields[EmptyTuple, EmptyTuple] = 
      new Fields(EmptyTuple)

  given [L <: String & Singleton: ValueOf,
         LT <: Tuple,
         V : ValueDecoder,
         VT <: Tuple
        ](using tailDecoder: ParquetRecordDecoder[Fields[LT, VT]])
        : ParquetRecordDecoder[Fields[L *: LT, V *: VT]] with
    def decode(record: RowParquetRecord, configuration: ValueCodecConfiguration): Fields[L *: LT, V *: VT] =
      val fieldName = summon[ValueOf[L]].value
      val decodedFieldOpt = try
        record.get[V](fieldName, configuration)
      catch
        case NonFatal(cause) =>
          throw DecodingException(s"Failed to decode field $fieldName of record: $record", cause)

      decodedFieldOpt match
        case Some(decodedFieldValue) =>
          Fields(decodedFieldValue *: tailDecoder.decode(record, configuration).values)
        case None => // TODO add test
          throw DecodingException(s"Field $fieldName is not in schema of: $record")
    

  given derived[P <: Product](using
                              mirror: Mirror.ProductOf[P],
                              decoder: ParquetRecordDecoder[Fields[mirror.MirroredElemLabels, mirror.MirroredElemTypes]]
                             ): ParquetRecordDecoder[P] with
    def decode(record: RowParquetRecord, configuration: ValueCodecConfiguration): P =
      mirror.fromProduct(decoder.decode(record, configuration).values)

end ParquetRecordDecoder
