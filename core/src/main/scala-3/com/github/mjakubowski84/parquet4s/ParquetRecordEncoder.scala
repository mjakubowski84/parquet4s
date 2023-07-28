package com.github.mjakubowski84.parquet4s

import scala.deriving.Mirror
import scala.annotation.implicitNotFound
import scala.util.control.NonFatal
import scala.reflect.ClassTag

/** Type class that allows to encode given data entity as [[RowParquetRecord]]
  * @tparam T
  *   type of source data
  */
@implicitNotFound(
  "Cannot write data of type ${T}. " +
    "Please check if there is implicit ValueEncoder available for each field and subfield of ${T}."
)
trait ParquetRecordEncoder[T]:
  /** @param entity
    *   data to be encoded
    * @param resolver
    *   data type used to resolve and cache empty instances of [[RowParquetRecord]] for given class
    * @param configuration
    *   [ValueCodecConfiguration] used by some codecs
    * @return
    *   [[RowParquetRecord]] containing product elements from the data
    */
  def encode(
      entity: T,
      resolver: EmptyRowParquetRecordResolver,
      configuration: ValueCodecConfiguration
  ): RowParquetRecord

object ParquetRecordEncoder:

  object EncodingException:
    def apply(msg: String, cause: Throwable): EncodingException =
      val encodingException = EncodingException(msg)
      encodingException.initCause(cause)
      encodingException

  case class EncodingException(msg: String) extends RuntimeException(msg)

  private[ParquetRecordEncoder] class Fields[Labels <: Tuple, Values <: Tuple](val values: Values)

  def apply[T](using ev: ParquetRecordEncoder[T]): ParquetRecordEncoder[T] = ev

  def encode[T](entity: T, configuration: ValueCodecConfiguration = ValueCodecConfiguration.Default)(using
      ev: ParquetRecordEncoder[T]
  ): RowParquetRecord = ev.encode(entity, null, configuration)

  given ParquetRecordEncoder[Fields[EmptyTuple, EmptyTuple]] with
    def encode(
        empty: Fields[EmptyTuple, EmptyTuple],
        resolver: EmptyRowParquetRecordResolver,
        configuration: ValueCodecConfiguration
    ): RowParquetRecord =
      resolver.resolveEmptyRowParquetRecord

  given [L <: String & Singleton: ValueOf, LT <: Tuple, V: ValueEncoder, VT <: Tuple](using
      tailEncoder: ParquetRecordEncoder[Fields[LT, VT]]
  ): ParquetRecordEncoder[Fields[L *: LT, V *: VT]] with
    def encode(
        fields: Fields[L *: LT, V *: VT],
        resolver: EmptyRowParquetRecordResolver,
        configuration: ValueCodecConfiguration
    ): RowParquetRecord =
      val fieldName = summon[ValueOf[L]].value
      val fieldValue =
        try summon[ValueEncoder[V]].encode(fields.values.head, configuration)
        catch
          case NonFatal(cause) =>
            throw EncodingException(
              s"Failed to encode field $fieldName: ${fields.values.head}, due to ${cause.getMessage}",
              cause
            )
      tailEncoder
        .encode(Fields(fields.values.tail), resolver.add(fieldName), configuration)
        .add(fieldName, fieldValue)

  given derived[P <: Product](using
      classTag: ClassTag[P],
      mirror: Mirror.ProductOf[P],
      encoder: ParquetRecordEncoder[Fields[mirror.MirroredElemLabels, mirror.MirroredElemTypes]]
  ): ParquetRecordEncoder[P] with
    def encode(
        product: P,
        resolver: EmptyRowParquetRecordResolver,
        configuration: ValueCodecConfiguration
    ): RowParquetRecord =
      encoder.encode(
        Fields(Tuple.fromProductTyped(product)),
        EmptyRowParquetRecordResolver(classTag.runtimeClass),
        configuration
      )

end ParquetRecordEncoder

sealed trait EmptyRowParquetRecordResolver:
  def add(fieldName: String): EmptyRowParquetRecordResolver
  def resolveEmptyRowParquetRecord: RowParquetRecord

object EmptyRowParquetRecordResolver:
  private[parquet4s] val Cache = scala.collection.concurrent.TrieMap.empty[Class[_], RowParquetRecord]

  def apply(clazz: Class[_]): EmptyRowParquetRecordResolver =
    Cache.get(clazz) match {
      case Some(record) => Cached(record)
      case None         => Builder(Vector.empty, clazz)
    }

  private class Cached(rowParquetRecord: RowParquetRecord) extends EmptyRowParquetRecordResolver:
    override def add(fieldName: String): EmptyRowParquetRecordResolver = this
    override def resolveEmptyRowParquetRecord: RowParquetRecord        = rowParquetRecord

  private class Builder(fields: Vector[String], clazz: Class[_]) extends EmptyRowParquetRecordResolver:
    override def add(fieldName: String): EmptyRowParquetRecordResolver =
      Builder(fields :+ fieldName, clazz)
    override def resolveEmptyRowParquetRecord: RowParquetRecord =
      Cache.getOrElseUpdate(clazz, RowParquetRecord.emptyWithSchema(fields))

end EmptyRowParquetRecordResolver
