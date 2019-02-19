package com.github.mjakubowski84.parquet4s

import java.nio.{ByteBuffer, ByteOrder}
import java.util.TimeZone

import org.scalatest.{FlatSpec, Matchers}
import ValueImplicits._
import TestCases._
import ParquetRecordEncoder.{EncodingException, encode}

class ParquetRecordEncoderSpec extends FlatSpec with Matchers {

  "Parquet record encoder" should "be used to encode empty record" in {
    encode(Empty()) should be(RowParquetRecord())
  }

  it should "encode record containing primitive values" in {
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

  it should "encode record containing time values" in {
    val date = java.time.LocalDate.of(2019, 1, 1)
    val time = java.time.LocalTime.of(0, 0, 0)
    val dateTime = java.time.LocalDateTime.of(date, time)

    val data = TimePrimitives(
      localDateTime = dateTime,
      sqlTimestamp = java.sql.Timestamp.valueOf(dateTime),
      localDate = date,
      sqlDate = java.sql.Date.valueOf(date)
    )

    val epochDays = date.toEpochDay.toInt

    val binaryDateTime = BinaryValue {
      val buf = ByteBuffer.allocate(12).order(ByteOrder.LITTLE_ENDIAN)
      buf.putLong(-TimeZone.getDefault.getRawOffset * TimeValueCodecs.NanosPerMilli) // time in nanos with milli offset due to time zone
      buf.putInt(epochDays + TimeValueCodecs.JulianDayOfEpoch)
      buf.array()
    }

    val record = RowParquetRecord(
      "localDateTime" -> binaryDateTime,
      "sqlTimestamp" -> binaryDateTime,
      "localDate" -> epochDays,
      "sqlDate" -> epochDays
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

  it should "encode record containing map of primitives" in {
    encode(ContainsMapOfPrimitives(Map("key" -> 1))) should be(
      RowParquetRecord("map" -> MapParquetRecord("key" -> 1))
    )
  }

  it should "throw exception when decoding map with null key" in {
    an[EncodingException] should be thrownBy encode(ContainsMapOfPrimitives(Map(null.asInstanceOf[String] -> 1)))
  }

  it should "encode record containing map of optional primitives" in {
    encode(ContainsMapOfOptionalPrimitives(
      Map("1" -> None, "2" -> Some(2))
    )) should be(RowParquetRecord(
      "map" -> MapParquetRecord("1" -> NullValue, "2" -> IntValue(2))
    ))
  }

  it should "encode record containing map of collections of primitives" in {
    encode(ContainsMapOfCollectionsOfPrimitives(
      Map("1" -> List.empty, "2" -> List(1, 2, 3))
    )) should be(RowParquetRecord(
      "map" -> MapParquetRecord("1" -> ListParquetRecord.empty, "2" -> ListParquetRecord(1, 2, 3))
    ))
  }

  it should "encode record containing nested record" in {
    encode(ContainsNestedClass(Nested(1))) should be(
      RowParquetRecord("nested" -> RowParquetRecord("int" -> 1))
    )
  }

  it should "encode record containing null value" in {
    encode(ContainsNestedClass(null)) should be(
      RowParquetRecord("nested" -> NullValue)
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

  it should "encode record containing map with records as value" in {
    val dataWithEmptyMap = ContainsMapOfNestedClassAsValue(Map.empty)
    val dataWithMap = ContainsMapOfNestedClassAsValue(Map("1" -> Nested(1), "2" -> Nested(2)))

    encode(dataWithEmptyMap) should be(RowParquetRecord("nested" -> MapParquetRecord.empty))

    val record = RowParquetRecord("nested" -> MapParquetRecord(
      "1" -> RowParquetRecord("int" -> 1),
      "2" -> RowParquetRecord("int" -> 2)
    ))
    encode(dataWithMap) should be(record)
  }

  it should "encode record containing map with records as key" in {
    val dataWithEmptyMap = ContainsMapOfNestedClassAsKey(Map.empty)
    val dataWithMap = ContainsMapOfNestedClassAsKey(Map(Nested(1) -> "1", Nested(2) -> "2"))

    encode(dataWithEmptyMap) should be(RowParquetRecord("nested" -> MapParquetRecord.empty))

    val record = RowParquetRecord("nested" -> MapParquetRecord(
      RowParquetRecord("int" -> 1) -> "1",
      RowParquetRecord("int" -> 2) -> "2"
    ))
    encode(dataWithMap) should be(record)
  }

  it should "encode record containing map with optional records as value" in {
    val dataWithEmptyMap = ContainsMapOfOptionalNestedClassAsValue(Map.empty)
    val dataWithMap = ContainsMapOfOptionalNestedClassAsValue(Map(
      "none" -> None,
      "some" -> Some(Nested(2))
    ))

    encode(dataWithEmptyMap) should be(RowParquetRecord("nested" -> MapParquetRecord.empty))

    val record = RowParquetRecord("nested" -> MapParquetRecord(
      "none" -> NullValue,
      "some" -> RowParquetRecord("int" -> 2)
    ))
    encode(dataWithMap) should be(record)
  }

  it should "encode record containing map with collection of records as value" in {
    val dataWithEmptyMap = ContainsMapOfCollectionsOfNestedClassAsValue(Map.empty)
    val dataWithMap = ContainsMapOfCollectionsOfNestedClassAsValue(Map(
      "empty" -> List.empty,
      "nonEmpty" -> List(Nested(1), Nested(2), Nested(3))
    ))

    encode(dataWithEmptyMap) should be(RowParquetRecord("nested" -> MapParquetRecord.empty))

    val record = RowParquetRecord("nested" -> MapParquetRecord(
      "empty" -> ListParquetRecord.empty,
      "nonEmpty" -> ListParquetRecord(RowParquetRecord("int" -> 1), RowParquetRecord("int" -> 2), RowParquetRecord("int" -> 3))
    ))
    encode(dataWithMap) should be(record)
  }

}
