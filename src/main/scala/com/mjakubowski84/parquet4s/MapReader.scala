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
