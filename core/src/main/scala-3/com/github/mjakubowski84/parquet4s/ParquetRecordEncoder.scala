package com.github.mjakubowski84.parquet4s


import scala.deriving.Mirror
import scala.annotation.implicitNotFound
import scala.util.control.NonFatal


/**
  * Type class that allows to encode given data entity as [[RowParquetRecord]]
  * @tparam T type of source data
  */
@implicitNotFound("Cannot write data of type ${T}. " +
  "Please check if there is implicit ValueEncoder available for each field and subfield of ${T}."
)
trait ParquetRecordEncoder[T]:
  /**
   * @param entity        data to be encoded
   * @param configuration [ValueCodecConfiguration] used by some codecs
   * @return [[RowParquetRecord]] containing product elements from the data
   */
  def encode(entity: T, configuration: ValueCodecConfiguration): RowParquetRecord

object ParquetRecordEncoder:

  object EncodingException:
    def apply(msg: String, cause: Throwable): EncodingException =
      val encodingException = EncodingException(msg)
      encodingException.initCause(cause)
      encodingException


  case class EncodingException(msg: String) extends RuntimeException(msg)

  private[ParquetRecordEncoder] class Fields[Labels <: Tuple, Values <: Tuple](val values: Values)

  def apply[T](using ev: ParquetRecordEncoder[T]): ParquetRecordEncoder[T] = ev

  def encode[T](entity: T, configuration: ValueCodecConfiguration = ValueCodecConfiguration.Default)
               (using ev: ParquetRecordEncoder[T]): RowParquetRecord = ev.encode(entity, configuration)

  given ParquetRecordEncoder[Fields[EmptyTuple, EmptyTuple]] with
    def encode(empty: Fields[EmptyTuple, EmptyTuple], configuration: ValueCodecConfiguration): RowParquetRecord = 
      RowParquetRecord.EmptyNoSchema

  given [L <: String & Singleton: ValueOf,
         LT <: Tuple,
         V : ValueEncoder,
         VT <: Tuple
        ](using tailEncoder: ParquetRecordEncoder[Fields[LT, VT]])
        : ParquetRecordEncoder[Fields[L *: LT, V *: VT]] with
    def encode(fields: Fields[L *: LT, V *: VT], configuration: ValueCodecConfiguration): RowParquetRecord =
      val fieldName = summon[ValueOf[L]].value
      val fieldValue = try {
        summon[ValueEncoder[V]].encode(fields.values.head, configuration)
      } catch {
        case NonFatal(cause) =>
          throw EncodingException(s"Failed to encode field $fieldName: ${fields.values.head}, due to ${cause.getMessage}", cause)
      }
      tailEncoder.encode(Fields(fields.values.tail), configuration).prepended(fieldName, fieldValue)
    
  given derived[P <: Product](using
                              mirror: Mirror.ProductOf[P],
                              encoder: ParquetRecordEncoder[Fields[mirror.MirroredElemLabels, mirror.MirroredElemTypes]]
                             ): ParquetRecordEncoder[P] with
    def encode(product: P, configuration: ValueCodecConfiguration): RowParquetRecord =
      encoder.encode(Fields(Tuple.fromProductTyped(product)), configuration)

end ParquetRecordEncoder
