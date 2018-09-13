package com.mjakubowski84.parquet4s

import cats.Monoid
import shapeless.labelled.{FieldType, field}
import shapeless.{::, HList, HNil, LabelledGeneric, Lazy, Witness}

import scala.collection.generic.CanBuildFrom
import scala.language.higherKinds


/**
  * TODO change from reading Map[String, Any] to reading ParquetRecord or P <: ParquetRecord
  */
trait MapReader[T] {
  def read(m: Map[String, Any]): T
}

object MapReader
  extends cats.instances.AllInstances
    with CanBuildFromList
    with AllValueDecoders {

  def apply[T](implicit ev: MapReader[T]): MapReader[T] = ev

  implicit val hnilReader: MapReader[HNil] = new MapReader[HNil] {
    override def read(m: Map[String, Any]): HNil.type = HNil
  }

  implicit def hconsValue[K <: Symbol, H, T <: HList](implicit
                                                      witness: Witness.Aux[K],
                                                      hDecoder: ValueDecoder[H],
                                                      hMonoid: Monoid[H],
                                                      tReader: MapReader[T]
                                                     ): MapReader[FieldType[K, H] :: T] = new MapReader[FieldType[K, H] :: T] {
    override def read(m: Map[String, Any]): FieldType[K, H] :: T = {
      val key = witness.value.name
      // TODO consider throwing exception in place of using a monoid in case of missing entry in map for the key
      val value = m.get(key).map(hDecoder.decode).getOrElse(hMonoid.empty)
      field[K](value) :: tReader.read(m)
    }
  }

  implicit def hconsProduct[K <: Symbol, H, T <: HList](implicit
                                                        witness: Witness.Aux[K],
                                                        hReader: Lazy[MapReader[H]],
                                                        tReader: MapReader[T]
                                                       ): MapReader[FieldType[K, H] :: T] = new MapReader[FieldType[K, H] :: T] {
    override def read(m: Map[String, Any]): FieldType[K, H] :: T = {
      val key = witness.value.name
      // TODO consider throwing exception if subMap field is missing (key is missing in m)
      val subMap = m.get(key).map(_.asInstanceOf[RowParquetRecord].getMap).getOrElse(Map.empty)
      val value = hReader.value.read(subMap)
      field[K](value) :: tReader.read(m)
    }
  }

  implicit def hconsCollectionProduct[K <: Symbol, H, Col[_], T <: HList](implicit
                                                                          witness: Witness.Aux[K],
                                                                          hReader: Lazy[MapReader[H]],
                                                                          tReader: MapReader[T],
                                                                          cbf: CanBuildFrom[List[H], H, Col[H]]
                                                       ): MapReader[FieldType[K, Col[H]] :: T] = new MapReader[FieldType[K, Col[H]] :: T] {
    override def read(m: Map[String, Any]): FieldType[K, Col[H]] :: T = {
      val key = witness.value.name
      val valuesOpt = m.get(key).map {
        case rowRecord: RowParquetRecord =>
          val listOfValues: List[H] = List(hReader.value.read(rowRecord.getMap))
          listOfValues.to[Col]
        case listRecord: ListParquetRecord =>
          val listOfSubMaps: List[Map[String, Any]] = listRecord.getList.map(_.asInstanceOf[RowParquetRecord].getMap)
          val listOfValues: List[H] = listOfSubMaps.map(hReader.value.read)
          listOfValues.to[Col]
        // TODO is it possible to have map record here?
      }
      val values = valuesOpt.getOrElse(cbf().result())
      field[K](values) :: tReader.read(m)
    }
  }

  // TODO support of map of nested classes?

  implicit def genericReader[A, R](implicit
                                   gen: LabelledGeneric.Aux[A, R],
                                   mapReader: Lazy[MapReader[R]]
                                  ): MapReader[A] = new MapReader[A] {
    override def read(m: Map[String, Any]): A = gen.from(mapReader.value.read(m))
  }
}

trait ParquetRecordDecoder[T, Record <: ParquetRecord] {

  def decode(record: Record): T

}

object ParquetRecordDecoder
  extends cats.instances.AllInstances
    with CanBuildFromList
    with AllValueDecoders {

  def apply[T, Record <: ParquetRecord](implicit ev: ParquetRecordDecoder[T, Record]): ParquetRecordDecoder[T, Record] = ev

  def decode[T, Record <: ParquetRecord](record: Record)(implicit ev: ParquetRecordDecoder[T, Record]): T = ev.decode(record)

  implicit def nilDecoder[Record <: ParquetRecord]: ParquetRecordDecoder[HNil, Record] = new ParquetRecordDecoder[HNil, Record] {
    override def decode(record: Record): HNil.type = HNil
  }

  implicit def headValueDecoder[Key <: Symbol, Head, Tail <: HList](implicit
                                                                    witness: Witness.Aux[Key],
                                                                    headDecoder: ValueDecoder[Head],
                                                                    headMonoid: Monoid[Head],
                                                                    tailDecoder: ParquetRecordDecoder[Tail, RowParquetRecord]
                                                                   ): ParquetRecordDecoder[FieldType[Key, Head] :: Tail, RowParquetRecord] =
    new ParquetRecordDecoder[FieldType[Key, Head] :: Tail, RowParquetRecord] {
      override def decode(record: RowParquetRecord): FieldType[Key, Head] :: Tail = {
        val key = witness.value.name
        // TODO consider throwing exception in place of using a monoid in case of missing entry in map for the key
        val value = record.getMap.get(key).map(headDecoder.decode).getOrElse(headMonoid.empty)
        field[Key](value) :: tailDecoder.decode(record)
      }
    }

  implicit def headProductDecoder[Key <: Symbol, Head, Tail <: HList](implicit
                                                                      witness: Witness.Aux[Key],
                                                                      headDecoder: Lazy[ParquetRecordDecoder[Head, RowParquetRecord]],
                                                                      tailDecoder: ParquetRecordDecoder[Tail, RowParquetRecord]
                                                                     ): ParquetRecordDecoder[FieldType[Key, Head] :: Tail, RowParquetRecord] =
    new ParquetRecordDecoder[FieldType[Key, Head] :: Tail, RowParquetRecord] {
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
                                                                                           headDecoder: Lazy[ParquetRecordDecoder[Head, RowParquetRecord]],
                                                                                           tailDecoder: ParquetRecordDecoder[Tail, RowParquetRecord],
                                                                                           cbf: CanBuildFrom[List[Head], Head, Col[Head]]
                                                                                          ): ParquetRecordDecoder[FieldType[Key, Col[Head]] :: Tail, RowParquetRecord] =
    new ParquetRecordDecoder[FieldType[Key, Col[Head]] :: Tail, RowParquetRecord] {
      override def decode(record: RowParquetRecord): FieldType[Key, Col[Head]] :: Tail = {
        val key = witness.value.name
        val values = record.getMap.get(key) match {
          case Some(rowRecord: RowParquetRecord) =>
            val listOfValues: List[Head] = List(headDecoder.value.decode(rowRecord))
            listOfValues.to[Col]
//            implicitly[ParquetRecordDecoder[Col[Head], RowParquetRecord]].decode(rowRecord)
          case Some(listRecord: ListParquetRecord) =>
            // TODO introduce listdecoder?
            val listOfNestedRecords: List[RowParquetRecord] = listRecord.getList.map(_.asInstanceOf[RowParquetRecord])
            val listOfValues: List[Head] = listOfNestedRecords.map(headDecoder.value.decode)
            listOfValues.to[Col]
//            implicitly[ParquetRecordDecoder[Col[Head], ListParquetRecord]].decode(listRecord)
          case Some(mapRecord: MapParquetRecord) =>
            // TODO proper exception with proper message
            throw new RuntimeException(s"Unexpected MapParquetRecord input: $mapRecord")
          case None =>
            cbf().result()
        }
        field[Key](values) :: tailDecoder.decode(record)
      }
    }

/*  implicit def smthFromList[A, Col[_]](implicit
                               headDecoder: Lazy[ParquetRecordDecoder[A, RowParquetRecord]],
                               cbf: CanBuildFrom[List[A], A, Col[A]]
                              ): ParquetRecordDecoder[Col[A], ListParquetRecord] =
    new ParquetRecordDecoder[Col[A], ListParquetRecord]() {
      override def decode(record: ListParquetRecord): Col[A] = {
        val listOfNestedRecords: List[RowParquetRecord] = record.getList.map(_.asInstanceOf[RowParquetRecord])
        val listOfValues: List[A] = listOfNestedRecords.map(headDecoder.value.decode)
        listOfValues.to[Col]
      }
    }
  implicit def smthFromProduct[A, Col[_]](implicit
                               headDecoder: Lazy[ParquetRecordDecoder[A, RowParquetRecord]],
                               cbf: CanBuildFrom[List[A], A, Col[A]]
                              ): ParquetRecordDecoder[Col[A], RowParquetRecord] =
    new ParquetRecordDecoder[Col[A], RowParquetRecord]() {
      override def decode(record: RowParquetRecord): Col[A] = {
        val listOfValues: List[A] = List(headDecoder.value.decode(record))
        listOfValues.to[Col]
      }
    }*/

  implicit def genericDecoder[A, R, Record <: ParquetRecord](implicit
                                                             gen: LabelledGeneric.Aux[A, R],
                                                             decoder: Lazy[ParquetRecordDecoder[R, Record]]
                                                            ): ParquetRecordDecoder[A, Record] =
    new ParquetRecordDecoder[A, Record] {
      override def decode(record: Record): A = gen.from(decoder.value.decode(record))
    }

}
