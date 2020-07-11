package com.github.mjakubowski84.parquet4s

import com.github.mjakubowski84.parquet4s.SkippingParquetRecordEncoder.encode
import com.github.mjakubowski84.parquet4s.ValueImplicits._
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class SkippingParquetRecordEncoderSpec  extends AnyFlatSpec with Matchers {

  case class Street(name: String, more: Option[String])
  case class Address(street: Street, city: String)
  case class Person(name: String, age: Int, address: Address)

  val person: Person = Person(
    name = "Joe",
    age = 18,
    address = Address(
      street = Street(name = "Broad St", more = Some("123")),
      city = "Somewhere"
    )
  )

  val personRecord: RowParquetRecord = RowParquetRecord(
    "name" -> "Joe",
    "age" -> 18,
    "address" -> RowParquetRecord(
      "street" -> RowParquetRecord(
        "name" -> "Broad St",
        "more" -> "123"
      ),
      "city" -> "Somewhere"
    )
  )

  "SkippingParquetRecordEncoder" should "skip a field at a long path" in {
    encode(Set("address.street.name"), person) should be(RowParquetRecord(
      "name" -> "Joe",
      "age" -> 18,
      "address" -> RowParquetRecord(
        "street" -> RowParquetRecord(
          "more" -> "123"
        ),
        "city" -> "Somewhere"
      )
    ))
  }

  it should "skip a mid path" in {
    encode(Set("address.street"), person) should be(RowParquetRecord(
      "name" -> "Joe",
      "age" -> 18,
      "address" -> RowParquetRecord(
        "city" -> "Somewhere"
      )
    ))
  }

  it should "skip all fields of case class" in {
    encode(Set("address.street.name", "address.street.more"), person) should be(RowParquetRecord(
      "name" -> "Joe",
      "age" -> 18,
      "address" -> RowParquetRecord(
        "city" -> "Somewhere"
      )
    ))
  }

  it should "skip the only field from a simple case class" in {
    case class Simple(field: String)
    encode(Set("field"), Simple("a")) should be(RowParquetRecord.empty)
  }

  it should "process class without partitioning" in {
    case class Simple(field: String)
    encode(Set.empty, Simple("a")) should be(RowParquetRecord("field" -> "a"))
  }

  it should "process empty class" in {
    case class Empty()
    encode(Set("field"), Empty()) should be(RowParquetRecord.empty)
  }

  "Generic SkippingParquetRecordEncoder" should "skip a field at a long path" in {
    encode(Set("address.street.name"), personRecord) should be(RowParquetRecord(
      "name" -> "Joe",
      "age" -> 18,
      "address" -> RowParquetRecord(
        "street" -> RowParquetRecord(
          "more" -> "123"
        ),
        "city" -> "Somewhere"
      )
    ))
  }

  it should "skip a mid path" in {
    encode(Set("address.street"), personRecord) should be(RowParquetRecord(
      "name" -> "Joe",
      "age" -> 18,
      "address" -> RowParquetRecord(
        "city" -> "Somewhere"
      )
    ))
  }

  it should "skip all fields of case class" in {
    encode(Set("address.street.name", "address.street.more"), personRecord) should be(RowParquetRecord(
      "name" -> "Joe",
      "age" -> 18,
      "address" -> RowParquetRecord(
        "city" -> "Somewhere"
      )
    ))
  }

  it should "skip the only field from a simple case class" in {
    case class Simple(field: String)
    encode(Set("field"), RowParquetRecord("field" -> "a")) should be(RowParquetRecord.empty)
  }

  it should "process class without partitioning" in {
    case class Simple(field: String)
    val record = RowParquetRecord("field" -> "a")
    encode(Set.empty, RowParquetRecord("field" -> "a")) should be(record)
  }

  it should "process empty class" in {
    case class Empty()
    encode(Set("field"), RowParquetRecord.empty) should be(RowParquetRecord.empty)
  }

}
