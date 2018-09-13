package com.mjakubowski84.parquet4s

import cats.Monoid
import shapeless.labelled.{FieldType, field}
import shapeless.{::, HList, HNil, LabelledGeneric, Lazy, Witness}

import scala.collection.generic.CanBuildFrom
import scala.language.higherKinds


trait ParquetRecordDecoder[T] {

  def decode(record: RowParquetRecord): T

}

object ParquetRecordDecoder
  extends cats.instances.AllInstances
    with CanBuildFromList
    with AllValueDecoders {

  def apply[T](implicit ev: ParquetRecordDecoder[T]): ParquetRecordDecoder[T] = ev

  def decode[T](record: RowParquetRecord)(implicit ev: ParquetRecordDecoder[T]): T = ev.decode(record)

  implicit val nilDecoder: ParquetRecordDecoder[HNil] = new ParquetRecordDecoder[HNil] {
    override def decode(record: RowParquetRecord): HNil.type = HNil
  }

  implicit def headValueDecoder[Key <: Symbol, Head, Tail <: HList](implicit
                                                                    witness: Witness.Aux[Key],
                                                                    headDecoder: ValueDecoder[Head],
                                                                    headMonoid: Monoid[Head],
                                                                    tailDecoder: ParquetRecordDecoder[Tail]
                                                                   ): ParquetRecordDecoder[FieldType[Key, Head] :: Tail] =
    new ParquetRecordDecoder[FieldType[Key, Head] :: Tail] {
      override def decode(record: RowParquetRecord): FieldType[Key, Head] :: Tail = {
        val key = witness.value.name
        // TODO consider throwing exception in place of using a monoid in case of missing entry in map for the key
        val value = record.getMap.get(key).map(headDecoder.decode).getOrElse(headMonoid.empty)
        field[Key](value) :: tailDecoder.decode(record)
      }
    }

  implicit def headProductDecoder[Key <: Symbol, Head, Tail <: HList](implicit
                                                                      witness: Witness.Aux[Key],
                                                                      headDecoder: Lazy[ParquetRecordDecoder[Head]],
                                                                      tailDecoder: ParquetRecordDecoder[Tail]
                                                                     ): ParquetRecordDecoder[FieldType[Key, Head] :: Tail] =
    new ParquetRecordDecoder[FieldType[Key, Head] :: Tail] {
      override def decode(record: RowParquetRecord): FieldType[Key, Head] :: Tail = {
        val key = witness.value.name
        record.getMap.get(key) match {
          case Some(nestedRecord: RowParquetRecord) =>
            val value = headDecoder.value.decode(nestedRecord)
            field[Key](value) :: tailDecoder.decode(record)
          case Some(other) =>
            // TODO introduce proper exception type and proper message
            throw new RuntimeException(s"$other of type ${other.getClass.getCanonicalName} is unexpected input for $key")
          case None =>
            // TODO introduce proper exception type and proper message
            throw new RuntimeException(s"Missing input for required field $key")
        }
      }
    }


  implicit def headCollectionOfProductsDecoder[Key <: Symbol, Head, Col[_], Tail <: HList](implicit
                                                                                           witness: Witness.Aux[Key],
                                                                                           headDecoder: Lazy[ParquetRecordDecoder[Head]],
                                                                                           tailDecoder: ParquetRecordDecoder[Tail],
                                                                                           cbf: CanBuildFrom[List[Head], Head, Col[Head]]
                                                                                          ): ParquetRecordDecoder[FieldType[Key, Col[Head]] :: Tail] =
    new ParquetRecordDecoder[FieldType[Key, Col[Head]] :: Tail] {
      override def decode(record: RowParquetRecord): FieldType[Key, Col[Head]] :: Tail = {
        val key = witness.value.name
        val values = record.getMap.get(key) match {
          case Some(rowRecord: RowParquetRecord) =>
            val listOfValues: List[Head] = List(headDecoder.value.decode(rowRecord))
            listOfValues.to[Col]
          case Some(listRecord: ListParquetRecord) =>
            val listOfNestedRecords: List[RowParquetRecord] = listRecord.getList.map(_.asInstanceOf[RowParquetRecord])
            val listOfValues: List[Head] = listOfNestedRecords.map(headDecoder.value.decode)
            listOfValues.to[Col]
          case Some(other) =>
            // TODO proper exception with proper message
            throw new RuntimeException(s"Unexpected input: $other")
          case None =>
            cbf().result()
        }
        field[Key](values) :: tailDecoder.decode(record)
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
