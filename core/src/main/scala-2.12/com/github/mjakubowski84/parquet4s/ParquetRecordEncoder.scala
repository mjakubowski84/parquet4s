package com.github.mjakubowski84.parquet4s

import shapeless.labelled.FieldType
import shapeless.{::, HList, HNil, LabelledGeneric, Lazy, Witness}

import scala.annotation.implicitNotFound
import scala.language.higherKinds
import scala.reflect.ClassTag
import scala.util.control.NonFatal


/**
  * Type class that allows to encode given data entity as [[RowParquetRecord]]
  * @tparam T type of source data
  */
@implicitNotFound("Cannot write data of type ${T}. " +
  "Please check if there is implicit ValueEncoder available for each field and subfield of ${T}."
)
trait ParquetRecordEncoder[T] {

  /**
    * @param entity data to be encoded
    * @param resolver data type used to resolve and cache empty instances of [[RowParquetRecord]] for given class
    * @param configuration [ValueCodecConfiguration] used by some codecs
    * @return [[RowParquetRecord]] containing product elements from the data
    */
  def encode(entity: T, resolver: EmptyRowParquetRecordResolver, configuration: ValueCodecConfiguration): RowParquetRecord

}

sealed trait EmptyRowParquetRecordResolver {
  def add(fieldName: String): EmptyRowParquetRecordResolver
  def resolveEmptyRowParquetRecord: RowParquetRecord
}

object EmptyRowParquetRecordResolver {
  private[parquet4s] val Cache = scala.collection.concurrent.TrieMap.empty[Class[_], RowParquetRecord]

  def apply(clazz: Class[_]): EmptyRowParquetRecordResolver =
    Cache.get(clazz) match {
      case Some(record) => new Cached(record)
      case None => new Builder(Vector.empty, clazz)
    }

  private class Cached(rowParquetRecord: RowParquetRecord) extends EmptyRowParquetRecordResolver {
    override def add(fieldName: String): EmptyRowParquetRecordResolver = this
    override def resolveEmptyRowParquetRecord: RowParquetRecord = rowParquetRecord
  }

  private class Builder(fields: Vector[String], clazz: Class[_]) extends EmptyRowParquetRecordResolver {
    override def add(fieldName: String): EmptyRowParquetRecordResolver =
      new Builder(fields :+ fieldName, clazz)
    override def resolveEmptyRowParquetRecord: RowParquetRecord =
      Cache.getOrElseUpdate(clazz, RowParquetRecord.emptyWithSchema(fields))
  }

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

  def encode[T](entity: T, configuration: ValueCodecConfiguration = ValueCodecConfiguration.Default)
               (implicit ev: ParquetRecordEncoder[T]): RowParquetRecord = ev.encode(entity, null, configuration)

  implicit val nilEncoder: ParquetRecordEncoder[HNil] = (_, resolver, _) => resolver.resolveEmptyRowParquetRecord

  implicit def headValueEncoder[FieldName <: Symbol, Head, Tail <: HList](implicit
                                                                          witness: Witness.Aux[FieldName],
                                                                          headEncoder: ValueEncoder[Head],
                                                                          tailEncoder: ParquetRecordEncoder[Tail]
                                                                         ): ParquetRecordEncoder[FieldType[FieldName, Head] :: Tail] =
    (entity: FieldType[FieldName, Head] :: Tail, resolver: EmptyRowParquetRecordResolver, configuration: ValueCodecConfiguration) => {
      val fieldName = witness.value.name
      val fieldValue = try {
        headEncoder.encode(entity.head, configuration)
      } catch {
        case NonFatal(cause) =>
          throw EncodingException(s"Failed to encode field $fieldName: ${entity.head}, due to ${cause.getMessage}", cause)
      }
      tailEncoder.encode(entity.tail, resolver.add(fieldName), configuration).add(fieldName, fieldValue)
    }

  implicit def genericEncoder[A, R](implicit
                                    classTag: ClassTag[A],
                                    gen: LabelledGeneric.Aux[A, R],
                                    encoder: Lazy[ParquetRecordEncoder[R]]
                                   ): ParquetRecordEncoder[A] =
    (entity: A, _, configuration: ValueCodecConfiguration) =>
      encoder.value.encode(gen.to(entity), EmptyRowParquetRecordResolver(classTag.runtimeClass), configuration)

}
