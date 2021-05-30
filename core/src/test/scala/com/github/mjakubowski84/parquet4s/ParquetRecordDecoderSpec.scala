package com.github.mjakubowski84.parquet4s

import com.github.mjakubowski84.parquet4s.ParquetRecordDecoder.{DecodingException, decode}
import com.github.mjakubowski84.parquet4s.TestCases._
import com.github.mjakubowski84.parquet4s.ValueImplicits._
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.nio.{ByteBuffer, ByteOrder}
import java.util.TimeZone


class ParquetRecordDecoderSpec extends AnyFlatSpec with Matchers {

  def dateTimeAsBinary(epochDays: Int, timeInNanos: Long, timeZone: TimeZone): BinaryValue =
    BinaryValue {
      val buf = ByteBuffer.allocate(12).order(ByteOrder.LITTLE_ENDIAN)
      // tz offset is expressed in millis while time in Parquet is expressed in nanos
      buf.putLong(-timeZone.getRawOffset * TimeValueCodecs.NanosPerMilli + timeInNanos)
      buf.putInt(epochDays + TimeValueCodecs.JulianDayOfEpoch)
      buf.array()
    }
  
  "Parquet record decoder" should "be used to decode empty record" in {
    decode[Empty](RowParquetRecord.EmptyNoSchema) should be(Empty())
  }

  it should "decode record containing primitive values" in {
    val data = Primitives(
      boolean = true,
      int = 1,
      long = 1234567890L,
      float = 1.1f,
      double = 1.00000000000001d,
      string = "text",
      short = 1,
      byte = 1,
      char = '\n',
      bigDecimal = BigDecimal.valueOf(1.00000000000001d)
    )
    val record = RowParquetRecord(
      "boolean" -> true.value,
      "int" -> 1.value,
      "long" -> 1234567890L.value,
      "float" -> 1.1f.value,
      "double" -> 1.00000000000001d.value,
      "string" -> "text".value,
      "short" -> (1: Short).value,
      "byte" -> (1: Byte).value,
      "char" -> '\n'.value,
      "bigDecimal" -> BigDecimal.valueOf(1.00000000000001d).value
    )
    decode[Primitives](record) should be(data)
  }

  it should "decode record containing time primitive values using local time zone" in {
    val timeZone: TimeZone = TimeZone.getDefault
    val date = java.time.LocalDate.of(2019, 1, 1)
    val time = java.time.LocalTime.of(0, 0, 0)
    val dateTime = java.time.LocalDateTime.of(date, time)

    val expectedData = TimePrimitives(
      localDateTime = dateTime,
      sqlTimestamp = java.sql.Timestamp.valueOf(dateTime),
      localDate = date,
      sqlDate = java.sql.Date.valueOf(date)
    )

    val epochDays = date.toEpochDay.toInt
    val binaryDateTime = dateTimeAsBinary(epochDays, time.toNanoOfDay, timeZone)
    val record = RowParquetRecord(
      "localDateTime" -> binaryDateTime,
      "sqlTimestamp" -> binaryDateTime,
      "localDate" -> epochDays.value,
      "sqlDate" -> epochDays.value
    )

    decode[TimePrimitives](record, ValueCodecConfiguration(timeZone)) should be(expectedData)
  }

  it should "decode record containing time primitive values using UTC time zone" in {
    val timeZone: TimeZone = TimeZone.getTimeZone("UTC")
    val date = java.time.LocalDate.of(2019, 1, 1)
    val time = java.time.LocalTime.of(0, 0, 0)
    val dateTime = java.time.LocalDateTime.of(date, time)

    val expectedData = TimePrimitives(
      localDateTime = dateTime,
      sqlTimestamp = java.sql.Timestamp.valueOf(dateTime),
      localDate = date,
      sqlDate = java.sql.Date.valueOf(date)
    )

    val epochDays = date.toEpochDay.toInt
    val binaryDateTime = dateTimeAsBinary(epochDays, time.toNanoOfDay, timeZone)
    val record = RowParquetRecord(
      "localDateTime" -> binaryDateTime,
      "sqlTimestamp" -> binaryDateTime,
      "localDate" -> epochDays.value,
      "sqlDate" -> epochDays.value
    )

    decode[TimePrimitives](record, ValueCodecConfiguration(timeZone)) should be(expectedData)
  }

  it should "decode record containing time primitive values using UTC time zone while input data was saved with time zone 1h east" in {
    val date = java.time.LocalDate.of(2019, 1, 1)
    val time = java.time.LocalTime.of(0, 0, 0)
    // 2018-12-31 23:00:00 local/no-tz
    val dateTime = java.time.LocalDateTime.of(date, time).minusHours(1)

    val expectedData = TimePrimitives(
      localDateTime = dateTime,
      sqlTimestamp = java.sql.Timestamp.valueOf(dateTime),
      localDate = date,
      sqlDate = java.sql.Date.valueOf(date)
    )

    val epochDays = date.toEpochDay.toInt
    // 2019-01-01 00:00:00 GMT+1
    val binaryDateTime = dateTimeAsBinary(epochDays, time.toNanoOfDay, TimeZone.getTimeZone("GMT+1"))
    val record = RowParquetRecord(
      "localDateTime" -> binaryDateTime,
      "sqlTimestamp" -> binaryDateTime,
      "localDate" -> epochDays.value,
      "sqlDate" -> epochDays.value
    )

    decode[TimePrimitives](record, ValueCodecConfiguration(TimeZone.getTimeZone("UTC"))) should be(expectedData)
  }


  it should "decode record containing time primitive values using UTC time zone while input data was saved with time zone 1h west" in {
    val date = java.time.LocalDate.of(2018, 12, 31)
    val time = java.time.LocalTime.of(23, 0, 0)
    // 2019-01-01 00:00:00 local/no-tz
    val dateTime = java.time.LocalDateTime.of(date, time).plusHours(1)

    val expectedData = TimePrimitives(
      localDateTime = dateTime,
      sqlTimestamp = java.sql.Timestamp.valueOf(dateTime),
      localDate = date,
      sqlDate = java.sql.Date.valueOf(date)
    )

    val epochDays = date.toEpochDay.toInt
    // 2018-12-31 23:00:00 GMT-1
    val binaryDateTime = dateTimeAsBinary(epochDays, time.toNanoOfDay, TimeZone.getTimeZone("GMT-1"))
    val record = RowParquetRecord(
      "localDateTime" -> binaryDateTime,
      "sqlTimestamp" -> binaryDateTime,
      "localDate" -> epochDays.value,
      "sqlDate" -> epochDays.value
    )

    decode[TimePrimitives](record, ValueCodecConfiguration(TimeZone.getTimeZone("UTC"))) should be(expectedData)
  }

  it should "throw exception if record is missing data for non-optional field" in {
    case class ContainsRequired(requiredField: Int)

    a[ParquetRecordDecoder.DecodingException] should be thrownBy decode[ContainsRequired](
      RowParquetRecord.EmptyNoSchema
    )
  }

  it should "decode record with optional field that has no value" in {
    decode[ContainsOption](RowParquetRecord.emptyWithSchema("optional")) should be(ContainsOption(None))
  }
  
  it should "throw exception if type of input field does not match expected type" in {
    case class ContainsInt(int: Int)

    val record = RowParquetRecord("int" -> "I am string but I should be int".value)

    a[ParquetRecordDecoder.DecodingException] should be thrownBy decode[ContainsInt](record)
  }

  it should "decode record containing collection of primitives" in {
    decode[Collections](RowParquetRecord(
      "list" -> ListParquetRecord.Empty,
      "seq" -> ListParquetRecord.Empty,
      "vector" -> ListParquetRecord.Empty,
      "set" -> ListParquetRecord.Empty,
      "array" -> ListParquetRecord.Empty
    )) should be(Collections(
      list = List.empty,
      seq = Seq.empty,
      vector = Vector.empty,
      set = Set.empty,
      array = Array.empty
    ))
    val listRecordWithValues = ListParquetRecord(1.value, 2.value, 3.value)
    decode[Collections](RowParquetRecord(
      "list" -> listRecordWithValues,
      "seq" -> listRecordWithValues,
      "vector" -> listRecordWithValues,
      "set" -> listRecordWithValues,
      "array" -> listRecordWithValues
    )) should be(Collections(
      list = List(1, 2, 3),
      seq = Seq(1, 2, 3),
      vector = Vector(1, 2, 3),
      set = Set(1, 2, 3),
      array = Array(1, 2, 3)
    ))
  }

  it should "decode record containing array of bytes" in {
    decode[ArrayOfBytes](RowParquetRecord(
      "bytes" -> BinaryValue(Array.empty[Byte])
    )) should be(ArrayOfBytes(Array.empty))
    val bytes = Array.apply[Byte](1, 2, 3)
    decode[ArrayOfBytes](RowParquetRecord(
      "bytes" -> BinaryValue(bytes)
    )) should be(ArrayOfBytes(bytes))
  }

  it should "decode record containing collection of optional primitives" in {
    decode[ContainsCollectionOfOptionalPrimitives](RowParquetRecord(
      "list" -> ListParquetRecord.Empty
    )) should be(
      ContainsCollectionOfOptionalPrimitives(List.empty)
    )
    decode[ContainsCollectionOfOptionalPrimitives](RowParquetRecord(
      "list" -> ListParquetRecord(NullValue, 2.value, NullValue))
    ) should be(
      ContainsCollectionOfOptionalPrimitives(List(None, Some(2), None))
    )
  }

  it should "decode record containing map of primitives" in {
    decode[ContainsMapOfPrimitives](
      RowParquetRecord("map" -> MapParquetRecord("key".value -> 1.value))
    ) should be(ContainsMapOfPrimitives(Map("key" -> 1)))
  }

  it should "throw exception when encountered null-value as a key" in {
    a[DecodingException] should be thrownBy {
      decode[ContainsMapOfPrimitives](RowParquetRecord("map" -> MapParquetRecord(NullValue -> 1.value)))
    }
  }

  it should "decode record containing map of optional primitives" in {
    decode[ContainsMapOfOptionalPrimitives](RowParquetRecord(
      "map" -> MapParquetRecord("1".value -> NullValue, "2".value -> IntValue(2))
    )) should be(ContainsMapOfOptionalPrimitives(
      Map("1" -> None, "2" -> Some(2))
    ))
  }

  it should "decode record containing map of collections of primitives" in {
    decode[ContainsMapOfCollectionsOfPrimitives](RowParquetRecord(
      "map" -> MapParquetRecord("1".value -> ListParquetRecord.Empty, "2".value -> ListParquetRecord(1.value, 2.value, 3.value))
    )) should be(ContainsMapOfCollectionsOfPrimitives(
      Map("1" -> List.empty, "2" -> List(1, 2, 3))
    ))
  }

  it should "decode record containing nested records" in {
    val data = ContainsNestedClass(Nested(1))
    val record = RowParquetRecord(
      "nested" -> RowParquetRecord("int" -> 1.value)
    )
    decode[ContainsNestedClass](record) should be(data)
  }

  it should "decode record that misses data for a nested record" in {
    decode[ContainsNestedClass](RowParquetRecord.emptyWithSchema("nested")) should be(ContainsNestedClass(null))
  }

  it should "throw exception if nested record does not match expected type" in {
    val record = RowParquetRecord("nested" -> ListParquetRecord(1.value))
    a[ParquetRecordDecoder.DecodingException] should be thrownBy decode[ContainsNestedClass](record)
  }

  it should "decode record containing optional nested record" in {
    val dataWithSome = ContainsOptionalNestedClass(Some(Nested(1)))
    val dataWithNone = ContainsOptionalNestedClass(None)

    decode[ContainsOptionalNestedClass](
      RowParquetRecord("nestedOptional" -> NullValue)
    ) should be(dataWithNone)

    decode[ContainsOptionalNestedClass](
      RowParquetRecord("nestedOptional" -> RowParquetRecord("int" -> 1.value))
    ) should be(dataWithSome)
  }

  it should "throw exception if optional nested record does not match expected type" in {
    val invalidRecordWithMap = RowParquetRecord("nestedOptional" -> MapParquetRecord("a".value -> 1.value))
    val invalidRecordWithList = RowParquetRecord("nestedOptional" -> ListParquetRecord(1.value))

    a[DecodingException] should be thrownBy decode[ContainsOptionalNestedClass](invalidRecordWithMap)
    a[DecodingException] should be thrownBy decode[ContainsOptionalNestedClass](invalidRecordWithList)
  }

  it should "decode record containing collection of nested records" in {
    decode[CollectionsOfNestedClass](RowParquetRecord(
      "list" -> ListParquetRecord.Empty,
      "seq" -> ListParquetRecord.Empty,
      "vector" -> ListParquetRecord.Empty,
      "set" -> ListParquetRecord.Empty,
      "array" -> ListParquetRecord.Empty
    )) should be(CollectionsOfNestedClass(
      list = List.empty,
      seq = Seq.empty,
      vector = Vector.empty,
      set = Set.empty,
      array = Array.empty
    ))

    val listOfNestedRecords = ListParquetRecord(
      RowParquetRecord("int" -> 1.value), RowParquetRecord("int" -> 2.value), RowParquetRecord("int" -> 3.value)
    )
    val expectedList = List(Nested(1), Nested(2), Nested(3))
    decode[CollectionsOfNestedClass](RowParquetRecord(
      "list" -> listOfNestedRecords,
      "seq" -> listOfNestedRecords,
      "vector" -> listOfNestedRecords,
      "set" -> listOfNestedRecords,
      "array" -> listOfNestedRecords
    )) should be(CollectionsOfNestedClass(
      list = expectedList,
      seq = expectedList,
      vector = expectedList.toVector,
      set = expectedList.toSet,
      array = expectedList.toArray
    ))
  }

  it should "throw exception if collection of nested records does not match expected type" in {
    val record = RowParquetRecord("set" -> MapParquetRecord("a".value -> 1.value))
    a[DecodingException] should be thrownBy decode[CollectionsOfNestedClass](record)
  }

  it should "decode record containing map with record as value" in {
    val dataWithEmptyMap = ContainsMapOfNestedClassAsValue(Map.empty)
    val dataWithMap = ContainsMapOfNestedClassAsValue(Map("1" -> Nested(1), "2" -> Nested(2)))

    decode[ContainsMapOfNestedClassAsValue](
      RowParquetRecord("nested" -> MapParquetRecord.Empty)
    ) should be(dataWithEmptyMap)

    val record = RowParquetRecord("nested" -> MapParquetRecord(
      "1".value -> RowParquetRecord("int" -> 1.value),
      "2".value -> RowParquetRecord("int" -> 2.value)
    ))
    decode[ContainsMapOfNestedClassAsValue](record) should be(dataWithMap)
  }

  it should "decode record containing map with record as key" in {
    val dataWithEmptyMap = ContainsMapOfNestedClassAsKey(Map.empty)
    val dataWithMap = ContainsMapOfNestedClassAsKey(Map(Nested(1) -> "1", Nested(2) -> "2"))

    decode[ContainsMapOfNestedClassAsKey](
      RowParquetRecord("nested" -> MapParquetRecord.Empty)
    ) should be(dataWithEmptyMap)

    val record = RowParquetRecord("nested" -> MapParquetRecord(
      RowParquetRecord("int" -> 1.value) -> "1".value,
      RowParquetRecord("int" -> 2.value) -> "2".value
    ))
    decode[ContainsMapOfNestedClassAsKey](record) should be(dataWithMap)
  }

  it should "throw exception when failed to decode a key of nested map record" in {
    val record = RowParquetRecord("nested" -> MapParquetRecord(123.value -> RowParquetRecord("int" -> 1.value)))
    a[DecodingException] should be thrownBy decode[ContainsMapOfNestedClassAsValue](record)
  }

  it should "throw exception when encountered implementation of ParquetRecord unsuitable for a map" in {
    val record = RowParquetRecord("nested" -> ListParquetRecord(RowParquetRecord("int" -> 1.value)))
    a[DecodingException] should be thrownBy decode[ContainsMapOfNestedClassAsValue](record)
  }

  it should "decode record containing map with optional records as value" in {
    val dataWithEmptyMap = ContainsMapOfOptionalNestedClassAsValue(Map.empty)
    val dataWithMap = ContainsMapOfOptionalNestedClassAsValue(Map(
      "none" -> None,
      "some" -> Some(Nested(2))
    ))

    decode[ContainsMapOfOptionalNestedClassAsValue](
      RowParquetRecord("nested" -> MapParquetRecord.Empty)
    ) should be(dataWithEmptyMap)

    val record = RowParquetRecord("nested" -> MapParquetRecord(
      "none".value -> NullValue,
      "some".value -> RowParquetRecord("int" -> 2.value)
    ))
    decode[ContainsMapOfOptionalNestedClassAsValue](record) should be(dataWithMap)
  }

  it should "decode record containing map with collection of records as value" in {
    val dataWithEmptyMap = ContainsMapOfCollectionsOfNestedClassAsValue(Map.empty)
    val dataWithMap = ContainsMapOfCollectionsOfNestedClassAsValue(Map(
      "empty" -> List.empty,
      "nonEmpty" -> List(Nested(1), Nested(2), Nested(3))
    ))

    decode[ContainsMapOfCollectionsOfNestedClassAsValue](
      RowParquetRecord("nested" -> MapParquetRecord.Empty)
    ) should be(dataWithEmptyMap)

    val record = RowParquetRecord("nested" -> MapParquetRecord(
      "empty".value -> ListParquetRecord.Empty,
      "nonEmpty".value -> ListParquetRecord(
        RowParquetRecord("int" -> 1.value),
        RowParquetRecord("int" -> 2.value),
        RowParquetRecord("int" -> 3.value)
      )
    ))
    decode[ContainsMapOfCollectionsOfNestedClassAsValue](record) should be(dataWithMap)
  }

}
