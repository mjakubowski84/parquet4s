package com.github.mjakubowski84.parquet4s

import shapeless.labelled.{FieldType, field}
import shapeless.{::, HList, HNil, LabelledGeneric, Lazy, Witness}

import scala.language.higherKinds
import scala.util.control.NonFatal


trait ParquetRecordDecoder[T] {

  def decode(value: Value): T

}

object ParquetRecordDecoder
  extends CollectionTransformers
    with AllValueCodecs {

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
            throw DecodingException(s"Unexpected value type: $other")
        }
      }
    }

  implicit def headProductDecoder[FieldName <: Symbol, Head, Tail <: HList](implicit
                                                                            witness: Witness.Aux[FieldName],
                                                                            headDecoder: Lazy[ParquetRecordDecoder[Head]],
                                                                            tailDecoder: ParquetRecordDecoder[Tail]
                                                                           ): ParquetRecordDecoder[FieldType[FieldName, Head] :: Tail] =
    new ParquetRecordDecoder[FieldType[FieldName, Head] :: Tail] {
      override def decode(value: Value): FieldType[FieldName, Head] :: Tail = {
        value match {
          case record: RowParquetRecord =>
            val fieldName = witness.value.name
            val fieldValue = record.getMap.getOrElse(fieldName, NullValue)
            val decodedFieldValue = try {
              headDecoder.value.decode(fieldValue)
            } catch {
              case NonFatal(cause) =>
                throw DecodingException(s"Failed to decode field $fieldName of record: $record", cause)
            }
            field[FieldName](decodedFieldValue) :: tailDecoder.decode(record)
          case other =>
            throw DecodingException(s"Unexpected value type: $other")
        }
      }
    }

  implicit def optionProductDecoder[V](implicit decoder: Lazy[ParquetRecordDecoder[V]]): ParquetRecordDecoder[Option[V]] =
    new ParquetRecordDecoder[Option[V]] {
      override def decode(value: Value): Option[V] = {
        value match {
          case NullValue =>
            None
          case _ =>
            Some(decoder.value.decode(value))
        }
      }
    }

  implicit def collectionProductDecoder[V, Col[_]](implicit
                                                   decoder: Lazy[ParquetRecordDecoder[V]],
                                                   collectionTransformer: CollectionTransformer[V, Col]
                                                  ): ParquetRecordDecoder[Col[V]] =
    new ParquetRecordDecoder[Col[V]] {
      override def decode(value: Value): Col[V] = {
        value match {
          case NullValue =>
            collectionTransformer.to(List.empty)
          case listRecord: ListParquetRecord =>
            val listOfValues = listRecord.elements.map(decoder.value.decode)
            collectionTransformer.to(listOfValues)
          case other =>
            throw DecodingException(s"Unexpected value type: $other")
        }
      }
    }

  implicit def mapProductDecoder[MapKey, MapValue](implicit
                                                   mapKeyDecoder: ValueCodec[MapKey],
                                                   mapValueDecoder: Lazy[ParquetRecordDecoder[MapValue]]
                                                  ): ParquetRecordDecoder[Map[MapKey, MapValue]] =
    new ParquetRecordDecoder[Map[MapKey, MapValue]] {
      override def decode(value: Value): Map[MapKey, MapValue] = {
        value match {
          case NullValue =>
            Map.empty
          case mapRecord: MapParquetRecord =>
            for ((mapKey, mapValue) <- mapRecord.getMap) yield {
              val decodedKey = try mapKeyDecoder.decode(mapKey) catch {
                case NonFatal(cause) =>
                  throw DecodingException(s"Failed to decode a map key $mapKey", cause)
              }
              decodedKey -> mapValueDecoder.value.decode(mapValue)
            }
          case other =>
            throw DecodingException(s"Unexpected value type: $other")
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
