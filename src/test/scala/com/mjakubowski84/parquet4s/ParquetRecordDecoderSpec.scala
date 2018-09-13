package com.mjakubowski84.parquet4s

import org.scalatest.{FlatSpec, Matchers}

class ParquetRecordDecoderSpec extends FlatSpec with Matchers {

  import ParquetRecordDecoder._

  "HNil decoder" should "be used to decode empty record" in {
    case class Empty()

    ParquetRecordDecoder.decode[Empty](RowParquetRecord()) should be(Empty())
  }

  "Primitive value decoder" should "decode record containing primitive values" in {
    case class Primitives(int: Int, string: String)

    val data = Primitives(1, "text")

    val record = RowParquetRecord("int" -> 1, "string" -> "text")

    ParquetRecordDecoder.decode[Primitives](record) should be(data)
  }

  "Product decoder" should "decode record containing nested records" in {
    case class NestedRow(int: Int, string: String)
    case class Row(nested1: NestedRow, nested2: NestedRow)

    val data = Row(NestedRow(1, "bla"), NestedRow(2, "ble"))

    val record = RowParquetRecord(
      "nested1" -> RowParquetRecord("int" -> 1, "string" -> "bla"),
      "nested2" -> RowParquetRecord("int" -> 2, "string" -> "ble")
    )

    ParquetRecordDecoder.decode[Row](record) should be(data)
  }

  "Collection of products decoder" should "decode record containing optional nested record" in {
    case class NestedRow(int: Int)
    case class Row(nestedOptional: Option[NestedRow])

    val dataWithSome = Row(Some(NestedRow(1)))
    val dataWithNone = Row(None)

    val record = RowParquetRecord()
    ParquetRecordDecoder.decode[Row](record) should be(dataWithNone)

    record.add("nestedOptional", RowParquetRecord("int" -> 1))
    ParquetRecordDecoder.decode[Row](record) should be(dataWithSome)
  }

  it should "decode record containing sequence of nested records" in {
    case class NestedRow(int: Int)
    case class Row(sequence: Seq[NestedRow])

    val dataWithSome = Row(Seq(NestedRow(1), NestedRow(2), NestedRow(3)))

    val listParquetRecord = ListParquetRecord(
      RowParquetRecord("int" -> 1),
      RowParquetRecord("int" -> 2),
      RowParquetRecord("int" -> 3)
    )

    val record = RowParquetRecord("sequence" -> listParquetRecord)

    ParquetRecordDecoder.decode[Row](record) should be(dataWithSome)
  }

  it should "decode record containing list of nested records" in {
    case class NestedRow(int: Int)
    case class Row(list: List[NestedRow])

    val dataWithSome = Row(List(NestedRow(1), NestedRow(2), NestedRow(3)))

    val listParquetRecord = ListParquetRecord(
      RowParquetRecord("int" -> 1),
      RowParquetRecord("int" -> 2),
      RowParquetRecord("int" -> 3)
    )

    val record = RowParquetRecord("list" -> listParquetRecord)

    ParquetRecordDecoder.decode[Row](record) should be(dataWithSome)
  }

  it should "decode record containing array of nested records" in {
    case class NestedRow(int: Int)
    case class Row(array: Array[NestedRow])

    val dataWithSome = Row(Array(NestedRow(1), NestedRow(2), NestedRow(3)))

    val listParquetRecord = ListParquetRecord(
      RowParquetRecord("int" -> 1),
      RowParquetRecord("int" -> 2),
      RowParquetRecord("int" -> 3)
    )

    val record = RowParquetRecord("array" -> listParquetRecord)

    ParquetRecordDecoder.decode[Row](record).array should be(dataWithSome.array)
  }

  it should "decode record containing vector of nested records" in {
    case class NestedRow(int: Int)
    case class Row(vector: Vector[NestedRow])

    val dataWithSome = Row(Vector(NestedRow(1), NestedRow(2), NestedRow(3)))

    val listParquetRecord = ListParquetRecord(
      RowParquetRecord("int" -> 1),
      RowParquetRecord("int" -> 2),
      RowParquetRecord("int" -> 3)
    )

    val record = RowParquetRecord("vector" -> listParquetRecord)

    ParquetRecordDecoder.decode[Row](record) should be(dataWithSome)
  }

  it should "decode record containing set of nested records" in {
    case class NestedRow(int: Int)
    case class Row(set: Set[NestedRow])

    val dataWithSome = Row(Set(NestedRow(1), NestedRow(2), NestedRow(3)))

    val listParquetRecord = ListParquetRecord(
      RowParquetRecord("int" -> 1),
      RowParquetRecord("int" -> 2),
      RowParquetRecord("int" -> 3)
    )

    val record = RowParquetRecord("set" -> listParquetRecord)

    ParquetRecordDecoder.decode[Row](record) should be(dataWithSome)
  }

}
