package com.github.mjakubowski84.parquet4s

import org.apache.parquet.io.api.Binary
import org.scalatest.{FlatSpec, Matchers}

class ParquetRecordSpec extends FlatSpec with Matchers {

  private val vcc = ValueCodecConfiguration.default

  "RowParquetRecord" should "indicate that is empty" in {
    val record = RowParquetRecord.empty
    record should be(empty)
    record should have size 0
  }

  it should "fail to get field from invalid index" in {
    an[NoSuchElementException] should be thrownBy RowParquetRecord.empty.head
    an[IndexOutOfBoundsException] should be thrownBy RowParquetRecord("a"-> IntValue(1))(2)
  }

  it should "succeed to add and retrieve a field" in {
    val record = RowParquetRecord.empty.add("a", "a", vcc).add("b", "b", vcc).add("c", "c", vcc)
    record.get[String]("a", vcc) should be("a")
    record.get[String]("b", vcc) should be("b")
    record.get[String]("c", vcc) should be("c")
    record.get[String]("d", vcc) should be(null)
    record.length should be(3)
  }

  it should "be updatable" in {
    val record = RowParquetRecord.empty.add("a", "a", vcc).add("b", "b", vcc).add("c", "c", vcc)
    record.update(0, BinaryValue("x".getBytes))
    record.update(1, ("z", BinaryValue("z".getBytes)))
    an[IndexOutOfBoundsException] should be thrownBy record.update(3, BinaryValue("z".getBytes))

    record should have size 3
    record.get[String]("a", vcc) should be("x")
    record.get[String]("z", vcc) should be("z")
    record.get[String]("c", vcc) should be("c")
  }

  "ListParquetRecord" should "indicate that is empty" in {
    val record = ListParquetRecord.empty
    record should be(empty)
    record should have size 0
  }

  "ListParquetRecord" should "accumulate normal primitive values" in {
    val lst = ListParquetRecord.empty
    val b1 = Binary.fromString("a string")
    val b2 = Binary.fromString("another string")
    lst.add("list", RowParquetRecord("element" -> BinaryValue(b1)))
    lst.add("list", RowParquetRecord("element" -> BinaryValue(b2)))
    lst should have size 2
    lst(0).asInstanceOf[BinaryValue].value should be(b1)
    lst(1).asInstanceOf[BinaryValue].value should be(b2)
  }

  "ListParquetRecord" should "accumulate normal compound values" in {
    val lst = ListParquetRecord.empty
    val r1 = RowParquetRecord("a"-> IntValue(1), "b"-> IntValue(2))
    val r2 = RowParquetRecord("c"-> IntValue(3), "d"-> IntValue(4))
    lst.add("list", RowParquetRecord("element" -> r1))
    lst.add("list", RowParquetRecord("element" -> r2))
    lst should have size 2
    lst(0).asInstanceOf[RowParquetRecord].get("a") should be(IntValue(1))
    lst(0).asInstanceOf[RowParquetRecord].get("b") should be(IntValue(2))
    lst(1).asInstanceOf[RowParquetRecord].get("c") should be(IntValue(3))
    lst(1).asInstanceOf[RowParquetRecord].get("d") should be(IntValue(4))
  }

  "ListParquetRecord" should "accumulate legacy primitive values" in {
    val lst = ListParquetRecord.empty
    val b1 = Binary.fromString("a string")
    val b2 = Binary.fromString("another string")
    lst.add("array",  BinaryValue(b1))
    lst.add("array",  BinaryValue(b2))
    lst should have size 2
    lst(0).asInstanceOf[BinaryValue].value should be(b1)
    lst(1).asInstanceOf[BinaryValue].value should be(b2)
  }

  "ListParquetRecord" should "accumulate legacy compound values" in {
    val lst = ListParquetRecord.empty
    val r1 = RowParquetRecord("a"-> IntValue(1), "b"-> IntValue(2))
    val r2 = RowParquetRecord("c"-> IntValue(3), "d"-> IntValue(4))
    lst.add("array", r1)
    lst.add("array", r2)
    lst should have size 2
    lst(0).asInstanceOf[RowParquetRecord].get("a") should be(IntValue(1))
    lst(0).asInstanceOf[RowParquetRecord].get("b") should be(IntValue(2))
    lst(1).asInstanceOf[RowParquetRecord].get("c") should be(IntValue(3))
    lst(1).asInstanceOf[RowParquetRecord].get("d") should be(IntValue(4))
  }

  "ListParquetRecord" should "accumulate null values" in {
    val lst = ListParquetRecord.empty
    val nofields = RowParquetRecord()
    lst.add("array", nofields)
    lst.add("array", nofields)
    lst.add("array", nofields)
    lst should have size 3
    for { elem <- lst } {
      elem should be(NullValue)
    }

  }


  it should "fail to get field from invalid index" in {
    an[NoSuchElementException] should be thrownBy ListParquetRecord.empty.head
    an[IndexOutOfBoundsException] should be thrownBy ListParquetRecord(IntValue(1))(2)
  }

  it should "succeed to add and retrieve a field" in {
    val record = ListParquetRecord.empty.add("a", vcc).add("b", vcc).add("c", vcc)
    record should have size 3
    record[String](0, vcc) should be("a")
    record[String](1, vcc) should be("b")
    record[String](2, vcc) should be("c")
  }

  it should "be updatable" in {
    val record = ListParquetRecord.empty.add("a", vcc).add("b", vcc).add("c", vcc)
    record.update(1, BinaryValue("x".getBytes))
    an[IndexOutOfBoundsException] should be thrownBy record.update(3, BinaryValue("z".getBytes))

    record should have size 3
    record[String](0, vcc) should be("a")
    record[String](1, vcc) should be("x")
    record[String](2, vcc) should be("c")
  }

  "MapParquetRecord" should "indicate that is empty" in {
    val record = MapParquetRecord.empty
    record should be(empty)
    record should have size 0
  }

  it should "fail to get field from invalid key" in {
    an[NoSuchElementException] should be thrownBy MapParquetRecord.empty.apply[Int, Int](1, vcc)
    an[NoSuchElementException] should be thrownBy MapParquetRecord(IntValue(1) -> IntValue(1)).apply[Int, Int](2, vcc)
  }

  it should "succeed to add and retrieve a field" in {
    val record = MapParquetRecord.empty.add(1, "a", vcc).add(2, "b", vcc).add(3, "c", vcc)
    record should have size 3
    record[Int, String](1, vcc) should be("a")
    record[Int, String](2, vcc) should be("b")
    record[Int, String](3, vcc) should be("c")
    record.get[Int, String](1, vcc) should be(Some("a"))
    record.get[Int, String](2, vcc) should be(Some("b"))
    record.get[Int, String](3, vcc) should be(Some("c"))
    record.get[Int, String](4, vcc) should be(None)
  }

  it should "be updatable" in {
    val record = MapParquetRecord.empty.add(1, "a", vcc).add(2, "b", vcc).add(3, "c", vcc)
    record.update[Int, String](2, "x", vcc)
    record.update[Int, String](4, "z", vcc)

    record should have size 4
    record[Int, String](1, vcc) should be("a")
    record[Int, String](2, vcc) should be("x")
    record[Int, String](3, vcc) should be("c")
    record[Int, String](4, vcc) should be("z")
  }

}
