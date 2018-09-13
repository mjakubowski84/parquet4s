package com.mjakubowski84.parquet4s

import org.scalatest.{FlatSpec, Matchers}

class ParquetRecordDecoderSpec extends FlatSpec with Matchers {

  import ParquetRecordDecoder._

  "HNil decoder" should "be used to decode empty record" in {
    case class Empty()

    ParquetRecordDecoder.decode[Empty, RowParquetRecord](new RowParquetRecord()) should be(Empty())
  }

  "Primitive value decoder" should "decode record containing primitive values" in {
    case class Primitives(int: Int, string: String)

    val data = Primitives(1, "text")

    val record = new RowParquetRecord()
    record.add("int", 1)
    record.add("string", "text")

    ParquetRecordDecoder.decode[Primitives, RowParquetRecord](record) should be(data)
  }

  "Product value decoder" should "decode record containing nested records" in {
    case class NestedRow(int: Int, string: String)
    case class Row(nested1: NestedRow, nested2: NestedRow)

    val data = Row(NestedRow(1, "bla"), NestedRow(2, "ble"))

    val nestedRecord1 = new RowParquetRecord()
    nestedRecord1.add("int", 1)
    nestedRecord1.add("string", "bla")
    val nestedRecord2 = new RowParquetRecord()
    nestedRecord2.add("int", 2)
    nestedRecord2.add("string", "ble")
    val record = new RowParquetRecord()
    record.add("nested1", nestedRecord1)
    record.add("nested2", nestedRecord2)

    ParquetRecordDecoder.decode[Row, RowParquetRecord](record) should be(data)
  }

}
