package com.github.mjakubowski84.parquet4s

import org.scalatest.{FlatSpec, Matchers}
import ValueImplicits._

class ParquetRecordEncoderSpec extends FlatSpec with Matchers {

  import ParquetRecordEncoder._ // TODO this import is needed only because of CollectionTransformers, maybe we need to change smth?

  "HNil encoder" should "be used to encode empty record" in {
    case class Empty()

    ParquetRecordEncoder.encode(Empty()) should be(RowParquetRecord())
  }

  "Value encoder" should "encode record containing primitive values" in {
    case class Primitives(int: Int, string: String)

    val data = Primitives(1, "text")
    val record = RowParquetRecord("int" -> 1, "string" -> "text")

    ParquetRecordEncoder.encode(data) should be(record)
  }

  it should "encode record containing optional values" in {
    case class Row(optionalField: Option[Int])

    ParquetRecordEncoder.encode(Row(None)) should be(RowParquetRecord("optionalField" -> NullValue))
    ParquetRecordEncoder.encode(Row(Some(1))) should be(RowParquetRecord("optionalField" -> 1))
  }

  it should "encode record containing collection of primitives" in {
    case class Row(list: List[Int])

    ParquetRecordEncoder.encode(Row(List.empty)) should be(RowParquetRecord("list" -> ListParquetRecord()))
    ParquetRecordEncoder.encode(Row(List(1, 2, 3))) should be(RowParquetRecord("list" -> ListParquetRecord(1, 2, 3)))
  }

  it should "encode record containing collection of optional primitives" in {
    case class Row(list: List[Option[Int]])

    ParquetRecordEncoder.encode(Row(List.empty)) should be(RowParquetRecord("list" -> ListParquetRecord()))
    ParquetRecordEncoder.encode(Row(List(None, Some(2), None))) should be(RowParquetRecord("list" -> ListParquetRecord(NullValue, 2, NullValue)))
  }

}
