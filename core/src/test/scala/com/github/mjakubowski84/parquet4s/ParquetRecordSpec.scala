package com.github.mjakubowski84.parquet4s

import org.apache.parquet.io.api.Binary
import org.scalatest.enablers.Sequencing
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.Inspectors
import org.scalatest.matchers.should.Matchers

import scala.collection.mutable

class ParquetRecordSpec extends AnyFlatSpec with Matchers with Inspectors {

  private val vcc = ValueCodecConfiguration.default
  private implicit val sequencing: Sequencing[mutable.Seq[Value]] = Sequencing.sequencingNatureOfGenSeq[Value, mutable.Seq]

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

  it should "succeed to add and retrieve a field using a path" in {
    val record = RowParquetRecord.empty
      .add("a", "a", vcc)
      .add(List("x", "y", "z"), BinaryValue("xyz".getBytes))
    record.get(List("a")) should be(BinaryValue("a".getBytes))
    record.get(List("a", "b")) should be(NullValue)
    record.get(List("k")) should be(NullValue)
    record.get(List("x")) should be(RowParquetRecord.empty.add(List("y", "z"), BinaryValue("xyz".getBytes)))
    record.get(List("x", "y")) should be(RowParquetRecord.empty.add("z", BinaryValue("xyz".getBytes)))
    record.get(List("x", "y", "z")) should be(BinaryValue("xyz".getBytes))
    record.length should be(2)
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

  it should "allow to remove fields by id" in {
    val record = RowParquetRecord.empty.add("a", "a", vcc).add("b", "b", vcc).add("c", "c", vcc)
    record.remove(2) should be(("c", BinaryValue("c".getBytes)))
    record.remove(0) should be(("a", BinaryValue("a".getBytes)))
    record.remove(0) should be(("b", BinaryValue("b".getBytes)))
    an[IndexOutOfBoundsException] should be thrownBy record.remove(0)
    record should be(empty)
  }

  it should "allow to remove fields by name" in {
    val record = RowParquetRecord.empty.add("a", "a", vcc).add("b", "b", vcc).add("c", "c", vcc)
    record.remove("c") should be(Some(BinaryValue("c".getBytes)))
    record.remove("c") should be(None)
    record should have size 2
  }

  it should "allow to remove fields by path" in {
    def createRecord() = RowParquetRecord.empty
      .add("a", "a", vcc)
      .add(List("x", "y", "z"), BinaryValue("xyz".getBytes))

    val r1 = createRecord()
    val r2 = createRecord()
    val r3 = createRecord()
    val r4 = createRecord()

    r1.remove(List("a")) should be(Some(BinaryValue("a".getBytes)))
    r1 should be(RowParquetRecord.empty.add(List("x", "y", "z"), BinaryValue("xyz".getBytes)))

    r2.remove(List("x")) should be(Some(RowParquetRecord.empty.add(List("y", "z"), BinaryValue("xyz".getBytes))))
    r2 should be(RowParquetRecord.empty.add("a", "a", vcc))

    r3.remove(List("x", "y")) should be(Some(RowParquetRecord.empty.add(List("z"), BinaryValue("xyz".getBytes))))
    r3 should be(RowParquetRecord.empty.add("a", "a", vcc))

    r4.remove(List("x", "y", "z")) should be(Some(BinaryValue("xyz".getBytes)))
    r4 should be(RowParquetRecord.empty.add("a", "a", vcc))
  }

  "ListParquetRecord" should "indicate that is empty" in {
    val record = ListParquetRecord.empty
    record should be(empty)
    record should have size 0
  }

  it should "accumulate normal primitive values" in {
    val b1 = BinaryValue(Binary.fromString("a string"))
    val b2 = BinaryValue(Binary.fromString("another string"))
    val lst = ListParquetRecord.empty
      .add("list", RowParquetRecord("element" -> b1))
      .add("list", RowParquetRecord("element" -> b2))
    lst should contain theSameElementsInOrderAs Seq(b1, b2)
  }

  it should "accumulate normal compound values" in {
    val r1 = RowParquetRecord("a"-> IntValue(1), "b"-> IntValue(2))
    val r2 = RowParquetRecord("c"-> IntValue(3), "d"-> IntValue(4))
    val lst = ListParquetRecord.empty
      .add("list", RowParquetRecord("element" -> r1))
      .add("list", RowParquetRecord("element" -> r2))
    lst should contain theSameElementsInOrderAs Seq(r1, r2)
  }

  it should "accumulate legacy primitive values" in {
    val b1 = BinaryValue(Binary.fromString("a string"))
    val b2 = BinaryValue(Binary.fromString("another string"))
    val lst = ListParquetRecord.empty.add("array",  b1).add("array",  b2)
    lst should contain theSameElementsInOrderAs Seq(b1, b2)
  }

  it should "accumulate legacy compound values" in {
    val r1 = RowParquetRecord("a"-> IntValue(1), "b"-> IntValue(2))
    val r2 = RowParquetRecord("c"-> IntValue(3), "d"-> IntValue(4))
    val lst = ListParquetRecord.empty.add("array", r1).add("array", r2)
    lst should contain theSameElementsInOrderAs Seq(r1, r2)
  }

  it should "accumulate null values" in {
    val lst = ListParquetRecord.empty
      .add("array", RowParquetRecord.empty)
      .add("array", RowParquetRecord.empty)
      .add("array", RowParquetRecord.empty)
    lst should have size 3
    every(lst) should be(NullValue)
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
