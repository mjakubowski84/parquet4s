package com.github.mjakubowski84.parquet4s

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
