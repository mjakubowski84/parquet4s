package com.github.mjakubowski84.parquet4s

import org.scalatest.enablers.Sequencing
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.Inspectors
import org.scalatest.matchers.should.Matchers
import com.github.mjakubowski84.parquet4s.ValueImplicits._

class ParquetRecordSpec extends AnyFlatSpec with Matchers with Inspectors {

  private val vcc                                                = ValueCodecConfiguration.Default
  implicit private val sequencing: Sequencing[ListParquetRecord] = Sequencing.sequencingNatureOfGenSeq[Value, Seq]

  "RowParquetRecord" should "indicate that is empty" in {
    val record = RowParquetRecord.EmptyNoSchema
    record should be(empty)
    record should have size 0
  }

  it should "fail to get field from invalid index" in {
    an[NoSuchElementException] should be thrownBy RowParquetRecord.EmptyNoSchema.head
    an[IndexOutOfBoundsException] should be thrownBy RowParquetRecord("a" -> 1.value)(2)
  }

  it should "succeed to add and retrieve a field" in {
    val record = RowParquetRecord
      .emptyWithSchema("a", "b", "c")
      .updated("a", "a", vcc)
      .updated("b", "b", vcc)
      .updated("c", "c", vcc)
    record.get[String]("a", vcc) should be(Some("a"))
    record.get[String]("b", vcc) should be(Some("b"))
    record.get[String]("c", vcc) should be(Some("c"))
    record.get[String]("d", vcc) should be(None)
    record.length should be(3)
  }

  it should "succeed to add and retrieve a field using a path" in {
    val record = RowParquetRecord
      .emptyWithSchema("a", "x")
      .updated("a", "a", vcc)
      .updated(Col("x.y.z"), "xyz".value)
      .updated(Col("x.v"), NullValue)

    record.get(Col("")) should be(Some(record))
    record.get(Col("a")) should be(Some("a".value))
    record.get(Col("a.b")) should be(None)
    record.get(Col("k")) should be(None)
    record.get(Col("x")) should be(Some(RowParquetRecord.fromColumns(Col("y.z") -> "xyz".value, Col("v") -> NullValue)))
    record.get(Col("x.y")) should be(Some(RowParquetRecord("z" -> "xyz".value)))
    record.get(Col("x.y.z")) should be(Some("xyz".value))
    record.get(Col("x.v")) should be(Some(NullValue))
    record.length should be(2)
  }

  it should "be updatable" in {
    val record = RowParquetRecord
      .emptyWithSchema("a", "b", "c")
      .updated("a", "a", vcc)
      .updated("b", "b", vcc)
      .updated("c", "c", vcc)
      .updated(0, "x".value)
      .updated(1, "z", "z".value)

    an[java.lang.IndexOutOfBoundsException] should be thrownBy record.updated(3, "z".value)

    record should have size 3
    record.get[String]("a", vcc) should be(Some("x"))
    record.get[String]("z", vcc) should be(Some("z"))
    record.get[String]("c", vcc) should be(Some("c"))
  }

  it should "allow to remove fields by id" in {
    val record = RowParquetRecord
      .emptyWithSchema("a", "b", "c")
      .updated("a", "a", vcc)
      .updated("b", "b", vcc)
      .updated("c", "c", vcc)

    an[IndexOutOfBoundsException] should be thrownBy record.removed(4)
    val (c, step1) = record.removed(2)
    val (a, step2) = step1.removed(0)
    val (b, step3) = step2.removed(0)

    c should be("c" -> "c".value)
    a should be("a" -> "a".value)
    b should be("b" -> "b".value)
    an[IndexOutOfBoundsException] should be thrownBy step3.removed(0)
    step3 should be(empty)
  }

  it should "allow to remove fields by name" in {
    val record = RowParquetRecord
      .emptyWithSchema("a", "b", "c")
      .updated("a", "a", vcc)
      .updated("b", "b", vcc)
      .updated("c", "c", vcc)

    val (cSome, step1) = record.removed("c")
    val (cNone, step2) = step1.removed("c")
    cSome should be(Some("c".value))
    cNone should be(None)
    step2 should have size 2
  }

  it should "allow to remove fields by path" in {
    val record = RowParquetRecord.fromColumns(
      Col("a") -> "a".value,
      Col("x.y.z") -> "xyz".value
    )

    record.removed(Col("")) should be(Some(record) -> RowParquetRecord.EmptyNoSchema)
    record.removed(Col("b")) should be(None -> record)
    record.removed(Col("x.q")) should be(None -> record)
    record.removed(Col("x.y.v")) should be(None -> record)
    record.removed(Col("x.y.z.q")) should be(None -> record)

    record.removed(Col("a")) should be(
      Some("a".value) -> RowParquetRecord.fromColumns(Col("x.y.z") -> "xyz".value)
    )

    record.removed(Col("x")) should be(
      Some(RowParquetRecord.fromColumns(Col("y.z") -> "xyz".value)) -> RowParquetRecord("a" -> "a".value)
    )

    record.removed(Col("x.y")) should be(
      Some(RowParquetRecord.fromColumns(Col("z") -> "xyz".value)) -> RowParquetRecord("a" -> "a".value)
    )

    record.removed(Col("x.y.z")) should be(
      Some("xyz".value) -> RowParquetRecord("a" -> "a".value)
    )
  }

  it should "iterate over values" in {
    val record = RowParquetRecord
      .emptyWithSchema("a", "b", "c")
      .updated("a", "a", vcc)
      .updated("b", "b", vcc)
      .updated("c", "c", vcc)
    record.iterator.toSeq should be(Seq("a" -> "a".value, "b" -> "b".value, "c" -> "c".value))
  }

  it should "provide NullValue for unset columns" in {
    val record = RowParquetRecord.emptyWithSchema("a", "b", "c")
    record.iterator.toSeq should be(Seq("a" -> NullValue, "b" -> NullValue, "c" -> NullValue))

    val updated = record.updated("b", "b", vcc)
    updated.head should be("a" -> NullValue)
    updated(1) should be("b" -> "b".value)
    updated(2) should be("c" -> NullValue)
    updated.get("b") should be(Some("b".value))
    updated.get("c") should be(Some(NullValue))
    updated.iterator.toSeq should be(Seq("a" -> NullValue, "b" -> "b".value, "c" -> NullValue))
  }

  "ListParquetRecord" should "indicate that is empty" in {
    val record = ListParquetRecord.Empty
    record should be(empty)
    record should have size 0
  }

  it should "accumulate normal primitive values" in {
    val b1 = "a string".value
    val b2 = "another string".value
    val lst = ListParquetRecord.Empty
      .add("list", RowParquetRecord("element" -> b1))
      .add("list", RowParquetRecord("element" -> b2))
    lst should contain theSameElementsInOrderAs Seq(b1, b2)
  }

  it should "accumulate normal compound values" in {
    val r1 = RowParquetRecord("a" -> 1.value, "b" -> 2.value)
    val r2 = RowParquetRecord("c" -> 3.value, "d" -> 4.value)
    val lst = ListParquetRecord.Empty
      .add("list", RowParquetRecord("element" -> r1))
      .add("list", RowParquetRecord("element" -> r2))
    lst should contain theSameElementsInOrderAs Seq(r1, r2)
  }

  it should "accumulate legacy primitive values" in {
    val b1  = "a string".value
    val b2  = "another string".value
    val lst = ListParquetRecord.Empty.add("array", b1).add("array", b2)
    lst should contain theSameElementsInOrderAs Seq(b1, b2)
  }

  it should "accumulate 3-levels legacy primitive values" in {
    val b1  = "a string".value
    val b2  = "another string".value
    val r1  = RowParquetRecord("array" -> b1)
    val r2  = RowParquetRecord("array" -> b2)
    val lst = ListParquetRecord.Empty.add("bag", r1).add("bag", r2)
    lst should contain theSameElementsInOrderAs Seq(b1, b2)
  }

  it should "accumulate 3-levels legacy primitive values with array_element" in {
    val b1  = "a string".value
    val b2  = "another string".value
    val r1  = RowParquetRecord("array_element" -> b1)
    val r2  = RowParquetRecord("array_element" -> b2)
    val lst = ListParquetRecord.Empty.add("bag", r1).add("bag", r2)
    lst should contain theSameElementsInOrderAs Seq(b1, b2)
  }

  it should "accumulate legacy compound values" in {
    val r1  = RowParquetRecord("a" -> 1.value, "b" -> 2.value)
    val r2  = RowParquetRecord("c" -> 3.value, "d" -> 4.value)
    val lst = ListParquetRecord.Empty.add("array", r1).add("array", r2)
    lst should contain theSameElementsInOrderAs Seq(r1, r2)
  }

  it should "accumulate null values" in {
    val lst = ListParquetRecord.Empty
      .add("array", RowParquetRecord.EmptyNoSchema)
      .add("array", RowParquetRecord.EmptyNoSchema)
      .add("array", RowParquetRecord.EmptyNoSchema)
    lst should have size 3
    every[Value, Seq](lst) should be(NullValue)
  }

  it should "fail to get field from invalid index" in {
    an[NoSuchElementException] should be thrownBy ListParquetRecord.Empty.head
    an[IndexOutOfBoundsException] should be thrownBy ListParquetRecord(1.value)(2)
  }

  it should "succeed to add and retrieve a field" in {
    val record = ListParquetRecord.Empty
      .appended("a", vcc)
      .appended("b", vcc)
      .appended("c", vcc)
    record should have size 3
    record[String](0, vcc) should be("a")
    record[String](1, vcc) should be("b")
    record[String](2, vcc) should be("c")
  }

  it should "be updatable" in {
    val record = ListParquetRecord.Empty
      .appended("a", vcc)
      .appended("b", vcc)
      .appended("c", vcc)
      .updated(1, "x".value)
    an[IndexOutOfBoundsException] should be thrownBy record.updated(3, "z".value)

    record should have size 3
    record[String](0, vcc) should be("a")
    record[String](1, vcc) should be("x")
    record[String](2, vcc) should be("c")
  }

  "MapParquetRecord" should "indicate that is empty" in {
    val record = MapParquetRecord.Empty
    record should be(empty)
    record should have size 0
  }

  it should "fail to get field from invalid key" in {
    a[NoSuchElementException] should be thrownBy MapParquetRecord.Empty.apply[Int, Int](1, vcc)
    a[NoSuchElementException] should be thrownBy MapParquetRecord(1.value -> 1.value).apply[Int, Int](2, vcc)
  }

  it should "succeed to add and retrieve a field" in {
    val record = MapParquetRecord.Empty
      .updated(1, "a", vcc)
      .updated(2, "b", vcc)
      .updated(3, "c", vcc)
    record should have size 3
    record[Int, String](1, vcc) should be("a")
    record[Int, String](2, vcc) should be("b")
    record[Int, String](3, vcc) should be("c")
    a[NoSuchElementException] should be thrownBy record[Int, String](4, vcc)
    record.get[Int, String](1, vcc) should be(Some("a"))
    record.get[Int, String](2, vcc) should be(Some("b"))
    record.get[Int, String](3, vcc) should be(Some("c"))
    record.get[Int, String](4, vcc) should be(None)
  }

  it should "be updatable" in {
    val record = MapParquetRecord.Empty
      .updated(1, "a", vcc)
      .updated(2, "b", vcc)
      .updated(3, "c", vcc)
      .updated(2, "x", vcc)
      .updated(4, "z", vcc)

    record should have size 4
    record[Int, String](1, vcc) should be("a")
    record[Int, String](2, vcc) should be("x")
    record[Int, String](3, vcc) should be("c")
    record[Int, String](4, vcc) should be("z")
  }

}
