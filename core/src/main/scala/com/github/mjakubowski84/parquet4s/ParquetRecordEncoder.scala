package com.github.mjakubowski84.parquet4s

import shapeless.labelled.FieldType
import shapeless.{::, HList, HNil, LabelledGeneric, Lazy, Witness}

import scala.language.higherKinds

trait ParquetRecordEncoder[T] {

  def encode(entity: T): RowParquetRecord

}

object ParquetRecordEncoder
  extends CollectionTransformers
    with AllValueCodecs {

  def apply[T](implicit ev: ParquetRecordEncoder[T]): ParquetRecordEncoder[T] = ev

  def encode[T](entity: T)(implicit ev: ParquetRecordEncoder[T]): RowParquetRecord = ev.encode(entity)

  implicit val nilDEncoder: ParquetRecordEncoder[HNil] = new ParquetRecordEncoder[HNil] {
    override def encode(nil: HNil): RowParquetRecord = RowParquetRecord()
  }

  implicit def headValueEncoder[FieldName <: Symbol, Head, Tail <: HList](implicit
                                                                          witness: Witness.Aux[FieldName],
                                                                          headEncoder: ValueCodec[Head],
                                                                          tailEncoder: ParquetRecordEncoder[Tail]
                                                                         ): ParquetRecordEncoder[FieldType[FieldName, Head] :: Tail] =
    new ParquetRecordEncoder[FieldType[FieldName, Head] :: Tail] {
      override def encode(entity: FieldType[FieldName, Head] :: Tail): RowParquetRecord = {
        val fieldName = witness.value.name
        val fieldValue = headEncoder.encode(entity.head)
        tailEncoder.encode(entity.tail).prepend(fieldName, fieldValue)
      }
    }


  implicit def headProductEncoder[FieldName <: Symbol, Head, Tail <: HList](implicit
                                                                            witness: Witness.Aux[FieldName],
                                                                            headEncoder: Lazy[ParquetRecordEncoder[Head]],
                                                                            tailEncoder: ParquetRecordEncoder[Tail]
                                                                           ): ParquetRecordEncoder[FieldType[FieldName, Head] :: Tail] =
    new ParquetRecordEncoder[FieldType[FieldName, Head] :: Tail] {
      override def encode(entity: FieldType[FieldName, Head] :: Tail): RowParquetRecord = {
        val fieldName = witness.value.name
        val fieldValue = headEncoder.value.encode(entity.head)
        tailEncoder.encode(entity.tail).prepend(fieldName, fieldValue)
      }
    }

  implicit def headCollectionOfProductsEncoder[FieldName <: Symbol, Head, Col[_], Tail <: HList](implicit
                                                                                                 witness: Witness.Aux[FieldName],
                                                                                                 headEncoder: Lazy[ParquetRecordEncoder[Head]],
                                                                                                 tailEncoder: ParquetRecordEncoder[Tail],
                                                                                                 collectionTransformer: CollectionTransformer[Head, Col]
                                                                                                 ): ParquetRecordEncoder[FieldType[FieldName, Col[Head]] :: Tail] =
    new ParquetRecordEncoder[FieldType[FieldName, Col[Head]] :: Tail] {
      override def encode(entity: FieldType[FieldName, Col[Head]] :: Tail): RowParquetRecord = {
        val fieldName = witness.value.name
        val listOfElements = collectionTransformer.from(entity.head)
        val listOfValues = listOfElements.map(headEncoder.value.encode)
        val record = ListParquetRecord(listOfValues:_*)
        tailEncoder.encode(entity.tail).prepend(fieldName, record)
      }
    }

  implicit def headOptionalProductEncoder[FieldName <: Symbol, Head, Tail <: HList](implicit
                                                                                                 witness: Witness.Aux[FieldName],
                                                                                                 headEncoder: Lazy[ParquetRecordEncoder[Head]],
                                                                                                 tailEncoder: ParquetRecordEncoder[Tail]
                                                                                                 ): ParquetRecordEncoder[FieldType[FieldName, Option[Head]] :: Tail] =
    new ParquetRecordEncoder[FieldType[FieldName, Option[Head]] :: Tail] {
      override def encode(entity: FieldType[FieldName, Option[Head]] :: Tail): RowParquetRecord = {
        val fieldName = witness.value.name
        entity.head.asInstanceOf[Option[Head]] match {
          case Some(element) =>
            tailEncoder.encode(entity.tail).prepend(fieldName, headEncoder.value.encode(element))
          case None =>
            tailEncoder.encode(entity.tail).prepend(fieldName, NullValue)
        }
      }
    }


  implicit def genericEncoder[A, R](implicit
                                    gen: LabelledGeneric.Aux[A, R],
                                    encoder: Lazy[ParquetRecordEncoder[R]]
                                   ): ParquetRecordEncoder[A] =
    new ParquetRecordEncoder[A] {
      override def encode(entity: A): RowParquetRecord = encoder.value.encode(gen.to(entity))
    }

}
