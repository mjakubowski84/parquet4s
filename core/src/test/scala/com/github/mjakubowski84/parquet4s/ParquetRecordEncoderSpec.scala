package com.github.mjakubowski84.parquet4s

import org.scalatest.{FlatSpec, Matchers}
import ValueImplicits._
import TestCases._
import ParquetRecordEncoder.encode

class ParquetRecordEncoderSpec extends FlatSpec with Matchers {

  import ParquetRecordEncoder._ // TODO this import is needed only because of CollectionTransformers, maybe we need to change smth?

  "HNil encoder" should "be used to encode empty record" in {
    encode(Empty()) should be(RowParquetRecord())
  }

  "Value encoder" should "encode record containing primitive values" in {
    val data = Primitives(
      boolean = true,
      int = 1,
      long = 1234567890l,
      float = 1.1f,
      double = 1.00000000000001d,
      string = "text"
    )
    val record = RowParquetRecord(
      "boolean" -> true,
      "int" -> 1,
      "long" -> 1234567890l,
      "float" -> 1.1f,
      "double" -> 1.00000000000001d,
      "string" -> "text"
    )
    encode(data) should be(record)
  }

  it should "encode record containing optional values" in {
    encode(ContainsOption(None)) should be(RowParquetRecord("optional" -> NullValue))
    encode(ContainsOption(Some(1))) should be(RowParquetRecord("optional" -> 1))
  }

  it should "encode record containing collection of primitives" in {
    encode(Collections(
      list = List.empty,
      seq = Seq.empty,
      vector = Vector.empty,
      set = Set.empty,
      array = Array.empty
    )) should be(RowParquetRecord(
      "list" -> ListParquetRecord.empty,
      "seq" -> ListParquetRecord.empty,
      "vector" -> ListParquetRecord.empty,
      "set" -> ListParquetRecord.empty,
      "array" -> ListParquetRecord.empty
    ))
    val listRecordWithValues = ListParquetRecord(1, 2, 3)
    encode(Collections(
      list = List(1, 2, 3),
      seq = Seq(1, 2, 3),
      vector = Vector(1, 2, 3),
      set = Set(1, 2, 3),
      array = Array(1, 2, 3)
    )) should be(RowParquetRecord(
      "list" -> listRecordWithValues,
      "seq" -> listRecordWithValues,
      "vector" -> listRecordWithValues,
      "set" -> listRecordWithValues,
      "array" -> listRecordWithValues
    ))
  }

  it should "encode record containing collection of optional primitives" in {
    encode(ContainsCollectionOfOptionalPrimitives(List.empty)) should be(
      RowParquetRecord("list" -> ListParquetRecord.empty)
    )
    encode(ContainsCollectionOfOptionalPrimitives(List(None, Some(2), None))) should be(
      RowParquetRecord("list" -> ListParquetRecord(NullValue, 2, NullValue))
    )
  }

  it should "encode record containing collection of collections" in {
    encode(ContainsCollectionOfCollections(List.empty)) should be(
      RowParquetRecord("listOfSets" -> ListParquetRecord.empty)
    )
    encode(ContainsCollectionOfCollections(List(Set.empty, Set(1, 2, 3), Set.empty))) should be(
      RowParquetRecord("listOfSets" -> ListParquetRecord(
        ListParquetRecord.empty,
        ListParquetRecord(1, 2, 3),
        ListParquetRecord.empty
      ))
    )
  }

  "Product encoder" should "encode record containing nested record" in {
    encode(ContainsNestedClass(Nested(1))) should be(
      RowParquetRecord("nested" -> RowParquetRecord("int" -> 1))
    )
  }

  it should "encode record containing optional nested record" in {
    encode(ContainsOptionalNestedClass(Some(Nested(1)))) should be(
      RowParquetRecord("nestedOptional" -> RowParquetRecord("int" -> 1))
    )
    encode(ContainsOptionalNestedClass(None)) should be(
      RowParquetRecord("nestedOptional" -> NullValue)
    )
  }

  it should "encode record containing collection of nested records" in {
    encode(CollectionsOfNestedClass(
      list = List.empty,
      seq = Seq.empty,
      vector = Vector.empty,
      set = Set.empty,
      array = Array.empty
    )) should be(RowParquetRecord(
      "list" -> ListParquetRecord.empty,
      "seq" -> ListParquetRecord.empty,
      "vector" -> ListParquetRecord.empty,
      "set" -> ListParquetRecord.empty,
      "array" -> ListParquetRecord.empty
    ))

    val listOfNestedRecords = ListParquetRecord(
      RowParquetRecord("int" -> 1), RowParquetRecord("int" -> 2), RowParquetRecord("int" -> 3)
    )
    encode(CollectionsOfNestedClass(
      list = List(Nested(1), Nested(2), Nested(3)),
      seq = Seq(Nested(1), Nested(2), Nested(3)),
      vector = Vector(Nested(1), Nested(2), Nested(3)),
      set = Set(Nested(1), Nested(2), Nested(3)),
      array = Array(Nested(1), Nested(2), Nested(3))
    )) should be(RowParquetRecord(
      "list" -> listOfNestedRecords,
      "seq" -> listOfNestedRecords,
      "vector" -> listOfNestedRecords,
      "set" -> listOfNestedRecords,
      "array" -> listOfNestedRecords
    ))
  }

}
