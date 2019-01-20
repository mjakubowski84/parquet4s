package com.github.mjakubowski84.parquet4s

import shapeless.labelled.{FieldType, field}
import shapeless.{::, HList, HNil, LabelledGeneric, Lazy, Witness}

import scala.language.higherKinds
import scala.util.control.NonFatal
import scala.util.{Failure, Success, Try}


trait ParquetRecordDecoder[T] {

  def decode(record: RowParquetRecord): T

}


trait CanBeEmpty[T] {
  def canBeEmpty: Boolean
  def emptyValue: T
}

trait CanBeEmpties {

  implicit def optCanBeEmpty[T]: CanBeEmpty[Option[T]] = new CanBeEmpty[Option[T]] {
    override val canBeEmpty: Boolean = true
    override val emptyValue: Option[T] = None
  }

  implicit def defaultCanBeEmpty[T]: CanBeEmpty[T] = new CanBeEmpty[T] {
    override val canBeEmpty: Boolean = false
    override def emptyValue: T = throw new UnsupportedOperationException("No empty value can be provided.")
  }

}

object ParquetRecordDecoder
  extends CollectionTransformers
    with CanBeEmpties
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
    override def decode(record: RowParquetRecord): HNil.type = HNil
  }

  implicit def headValueDecoder[FieldName <: Symbol, Head, Tail <: HList](implicit
                                                                          witness: Witness.Aux[FieldName],
                                                                          headDecoder: ValueCodec[Head],
                                                                          tailDecoder: ParquetRecordDecoder[Tail],
                                                                          canBeEmpty: CanBeEmpty[Head]
                                                                         ): ParquetRecordDecoder[FieldType[FieldName, Head] :: Tail] =
    new ParquetRecordDecoder[FieldType[FieldName, Head] :: Tail] {
      override def decode(record: RowParquetRecord): FieldType[FieldName, Head] :: Tail = {
        val fieldName = witness.value.name
        val value = record.getMap.get(fieldName).map(value => Try(headDecoder.decode(value))) match {
          case Some(Success(decodedValue)) =>
            decodedValue
          case Some(Failure(cause)) =>
            throw DecodingException(s"Failed to decode field $fieldName in record: $record", cause)
          case None if canBeEmpty.canBeEmpty =>
            canBeEmpty.emptyValue
          case None =>
            throw DecodingException(s"Missing field $fieldName in record: $record")
        }
        field[FieldName](value) :: tailDecoder.decode(record)
      }
    }

  implicit def headProductDecoder[FieldName <: Symbol, Head, Tail <: HList](implicit
                                                                            witness: Witness.Aux[FieldName],
                                                                            headDecoder: Lazy[ParquetRecordDecoder[Head]],
                                                                            tailDecoder: ParquetRecordDecoder[Tail]
                                                                           ): ParquetRecordDecoder[FieldType[FieldName, Head] :: Tail] =
    new ParquetRecordDecoder[FieldType[FieldName, Head] :: Tail] {
      override def decode(record: RowParquetRecord): FieldType[FieldName, Head] :: Tail = {
        val fieldName = witness.value.name
        record.getMap.get(fieldName) match {
          case Some(nestedRecord: RowParquetRecord) =>
            val value = headDecoder.value.decode(nestedRecord)
            field[FieldName](value) :: tailDecoder.decode(record)
          case Some(other) =>
            throw DecodingException(s"$other is unexpected input for field $fieldName in record: $record")
          case None =>
            throw DecodingException(s"Missing field $fieldName in record: $record")
        }
      }
    }

  implicit def headCollectionOfProductsDecoder[FieldName <: Symbol, Head, Col[_], Tail <: HList](implicit
                                                                                                 witness: Witness.Aux[FieldName],
                                                                                                 headDecoder: Lazy[ParquetRecordDecoder[Head]],
                                                                                                 tailDecoder: ParquetRecordDecoder[Tail],
                                                                                                 collectionTransformer: CollectionTransformer[Head, Col]
                                                                                                ): ParquetRecordDecoder[FieldType[FieldName, Col[Head]] :: Tail] =
    new ParquetRecordDecoder[FieldType[FieldName, Col[Head]] :: Tail] {
      override def decode(record: RowParquetRecord): FieldType[FieldName, Col[Head]] :: Tail = {
        val fieldName = witness.value.name
        val values = record.getMap.get(fieldName) match {
          case Some(listRecord: ListParquetRecord) =>
            val listOfNestedRecords: List[RowParquetRecord] = listRecord.elements.map(_.asInstanceOf[RowParquetRecord])
            val listOfValues: List[Head] = listOfNestedRecords.map(headDecoder.value.decode)
            collectionTransformer.to(listOfValues)
          case Some(other) =>
            throw DecodingException(s"$other is unexpected input for field $fieldName in record: $record")
          case None =>
            collectionTransformer.to(List.empty[Head])
        }
        field[FieldName](values) :: tailDecoder.decode(record)
      }
    }

  implicit def headOptionalProductDecoder[FieldName <: Symbol, Head, Tail <: HList](implicit
                                                                                    witness: Witness.Aux[FieldName],
                                                                                    headDecoder: Lazy[ParquetRecordDecoder[Head]],
                                                                                    tailDecoder: ParquetRecordDecoder[Tail]
                                                                                   ): ParquetRecordDecoder[FieldType[FieldName, Option[Head]] :: Tail] =
    new ParquetRecordDecoder[FieldType[FieldName, Option[Head]] :: Tail] {
      override def decode(record: RowParquetRecord): FieldType[FieldName, Option[Head]] :: Tail = {
        val fieldName = witness.value.name
        val values = record.getMap.get(fieldName) match {
          case Some(rowRecord: RowParquetRecord) =>
            Option(headDecoder.value.decode(rowRecord))
          case Some(other) =>
            throw DecodingException(s"$other is unexpected input for optional field $fieldName in record: $record")
          case None =>
            None
        }
        field[FieldName](values) :: tailDecoder.decode(record)
      }
    }

  implicit def headMapOfProductsDecoder[FieldName <: Symbol, Key, Value, Tail <: HList](implicit
                                                                                        witness: Witness.Aux[FieldName],
                                                                                        mapKeyDecoder: ValueCodec[Key],
                                                                                        mapValueDecoder: Lazy[ParquetRecordDecoder[Value]],
                                                                                        tailDecoder: ParquetRecordDecoder[Tail]
                                                                                      ): ParquetRecordDecoder[FieldType[FieldName, Map[Key, Value]] :: Tail] =
    new ParquetRecordDecoder[FieldType[FieldName, Map[Key, Value]] :: Tail] {
      override def decode(record: RowParquetRecord): FieldType[FieldName, Map[Key, Value]] :: Tail = {
        val fieldName = witness.value.name
        val values = record.getMap.get(fieldName) match {
          case Some(mapRecord: MapParquetRecord) =>
            for ((mapKey, mapValue: RowParquetRecord) <- mapRecord.getMap) yield {
              val decodedKey = try mapKeyDecoder.decode(mapKey) catch {
                case NonFatal(cause) =>
                  throw DecodingException(s"Failed to decode a key $mapKey of a map field $fieldName in record: $record", cause)
              }
              decodedKey -> mapValueDecoder.value.decode(mapValue)
            }
          case Some(other) =>
            throw DecodingException(s"$other is unexpected input for field $fieldName in record: $record")
          case None =>
            Map.empty[Key, Value]
        }
        field[FieldName](values) :: tailDecoder.decode(record)
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
