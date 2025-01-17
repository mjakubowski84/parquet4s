package com.github.mjakubowski84.parquet4s

import com.github.mjakubowski84.parquet4s.ValueImplicits.*
import org.scalatest.BeforeAndAfter
import org.scalatest.freespec.AnyFreeSpec
import org.scalatest.matchers.should.Matchers

class ParquetReaderItSpec extends AnyFreeSpec with Matchers with TestUtils with BeforeAndAfter {

  case class Partitioned(a: String, b: String, i: Int)
  case class I(i: Int)
  case class NestedPartitioned(nested: Nested, i: Int)
  case class Nested(b: String)

  before {
    clearTemp()
  }

  "Reading generic records from schema containing nullable fields should result in NullValues for missing data" in {
    case class Record(i: Int, d: Option[Double], nested: Nested)
    case class Nested(s: String)

    val path = Path(tempPath, "file.parquet")

    val records =
      Seq(
        Record(1, Some(2.1), Nested("non-null")),
        Record(1, None, Nested(null)),
        Record(1, None, null)
      )
    ParquetWriter.of[Record].writeAndClose(path, records)

    val readRecords = ParquetReader.generic.read(path)
    try
      readRecords.toSeq should be(
        Seq(
          RowParquetRecord("i" -> 1.value, "d" -> 2.1.value, "nested" -> RowParquetRecord("s" -> "non-null".value)),
          RowParquetRecord("i" -> 1.value, "d" -> NullValue, "nested" -> RowParquetRecord("s" -> NullValue)),
          RowParquetRecord("i" -> 1.value, "d" -> NullValue, "nested" -> NullValue)
        )
      )
    finally readRecords.close()
  }

  "Partition values should be set in read record" in {
    ParquetWriter.of[I].writeAndClose(Path(tempPath, "a=a1/b=b1/file.parquet"), Seq(I(1)))
    ParquetWriter.of[I].writeAndClose(Path(tempPath, "a=a1/b=b2/file.parquet"), Seq(I(2)))
    ParquetWriter.of[I].writeAndClose(Path(tempPath, "a=a2/b=b1/file.parquet"), Seq(I(3)))
    ParquetWriter.of[I].writeAndClose(Path(tempPath, "a=a2/b=b2/file.parquet"), Seq(I(4)))

    val partitioned = ParquetReader.as[Partitioned].read(tempPath)
    try
      partitioned.toSeq should contain theSameElementsAs
        Seq(
          Partitioned(a = "a1", b = "b1", i = 1),
          Partitioned(a = "a1", b = "b2", i = 2),
          Partitioned(a = "a2", b = "b1", i = 3),
          Partitioned(a = "a2", b = "b2", i = 4)
        )
    finally partitioned.close()
  }

  "Nested partition values should be set in read record and the projection shall be applied" in {
    ParquetWriter.of[I].writeAndClose(Path(tempPath, "a=a1/nested.b=b1/file.parquet"), Seq(I(1)))
    ParquetWriter.of[I].writeAndClose(Path(tempPath, "a=a1/nested.b=b2/file.parquet"), Seq(I(2)))
    ParquetWriter.of[I].writeAndClose(Path(tempPath, "a=a2/nested.b=b1/file.parquet"), Seq(I(3)))
    ParquetWriter.of[I].writeAndClose(Path(tempPath, "a=a2/nested.b=b2/file.parquet"), Seq(I(4)))

    val partitioned = ParquetReader.projectedAs[NestedPartitioned].read(tempPath)
    try
      partitioned.toSeq should contain theSameElementsAs
        Seq(
          NestedPartitioned(Nested(b = "b1"), i = 1),
          NestedPartitioned(Nested(b = "b2"), i = 2),
          NestedPartitioned(Nested(b = "b1"), i = 3),
          NestedPartitioned(Nested(b = "b2"), i = 4)
        )
    finally partitioned.close()
  }

  "Projection shall be applied to partition values when reading generic records" in {
    ParquetWriter.of[I].writeAndClose(Path(tempPath, "a=a1/nested.b=b1/file.parquet"), Seq(I(1)))
    ParquetWriter.of[I].writeAndClose(Path(tempPath, "a=a1/nested.b=b2/file.parquet"), Seq(I(2)))
    ParquetWriter.of[I].writeAndClose(Path(tempPath, "a=a2/nested.b=b1/file.parquet"), Seq(I(3)))
    ParquetWriter.of[I].writeAndClose(Path(tempPath, "a=a2/nested.b=b2/file.parquet"), Seq(I(4)))

    val schema = ParquetSchemaResolver.resolveSchema[NestedPartitioned]

    val partitioned = ParquetReader.projectedGeneric(schema).read(tempPath)
    try
      partitioned.toSeq should contain theSameElementsAs
        Seq(
          RowParquetRecord("nested" -> RowParquetRecord("b" -> "b1".value), "i" -> 1.value),
          RowParquetRecord("nested" -> RowParquetRecord("b" -> "b2".value), "i" -> 2.value),
          RowParquetRecord("nested" -> RowParquetRecord("b" -> "b1".value), "i" -> 3.value),
          RowParquetRecord("nested" -> RowParquetRecord("b" -> "b2".value), "i" -> 4.value)
        )
    finally partitioned.close()
  }

  "Partition values shall be completyly skipped if projection doesn't include them" in {
    ParquetWriter.of[I].writeAndClose(Path(tempPath, "a=a1/b=b1/file.parquet"), Seq(I(1)))
    ParquetWriter.of[I].writeAndClose(Path(tempPath, "a=a1/b=b2/file.parquet"), Seq(I(2)))
    ParquetWriter.of[I].writeAndClose(Path(tempPath, "a=a2/b=b1/file.parquet"), Seq(I(3)))
    ParquetWriter.of[I].writeAndClose(Path(tempPath, "a=a2/b=b2/file.parquet"), Seq(I(4)))

    val results = ParquetReader.projectedAs[I].read(tempPath)
    try
      results.toSeq should contain theSameElementsAs Seq(I(i = 1), I(i = 2), I(i = 3), I(i = 4))
    finally results.close()
  }

  "Partition values shall be completyly skipped if projection with generic records doesn't include them" in {
    ParquetWriter.of[I].writeAndClose(Path(tempPath, "a=a1/b=b1/file.parquet"), Seq(I(1)))
    ParquetWriter.of[I].writeAndClose(Path(tempPath, "a=a1/b=b2/file.parquet"), Seq(I(2)))
    ParquetWriter.of[I].writeAndClose(Path(tempPath, "a=a2/b=b1/file.parquet"), Seq(I(3)))
    ParquetWriter.of[I].writeAndClose(Path(tempPath, "a=a2/b=b2/file.parquet"), Seq(I(4)))

    val schema = ParquetSchemaResolver.resolveSchema[I]

    val results = ParquetReader.projectedGeneric(schema).read(tempPath)
    try
      results.toSeq should contain theSameElementsAs Seq(
        RowParquetRecord("i" -> 1.value),
        RowParquetRecord("i" -> 2.value),
        RowParquetRecord("i" -> 3.value),
        RowParquetRecord("i" -> 4.value)
      )
    finally results.close()
  }

  "Partitions should be filtered" in {
    ParquetWriter.of[I].writeAndClose(Path(tempPath, "a=a1/b=b1/file.parquet"), Seq(I(1)))
    ParquetWriter.of[I].writeAndClose(Path(tempPath, "a=a1/b=b2/file.parquet"), Seq(I(2)))
    ParquetWriter.of[I].writeAndClose(Path(tempPath, "a=a2/b=b1/file.parquet"), Seq(I(3)))
    ParquetWriter.of[I].writeAndClose(Path(tempPath, "a=a2/b=b2/file.parquet"), Seq(I(4)))

    val partitioned = ParquetReader.as[Partitioned].filter(Col("b") === "b1").read(tempPath)
    try
      partitioned.toSeq should contain theSameElementsAs
        Seq(
          Partitioned(a = "a1", b = "b1", i = 1),
          Partitioned(a = "a2", b = "b1", i = 3)
        )
    finally partitioned.close()
  }

  "Attempt to read inconsistent partitions should fail with an exception" in {
    ParquetWriter.of[I].writeAndClose(Path(tempPath, "a=a1/b=b1/file.parquet"), Seq(I(1)))
    ParquetWriter.of[I].writeAndClose(Path(tempPath, "a=a2/file.parquet"), Seq(I(2)))

    an[IllegalArgumentException] should be thrownBy ParquetReader.as[Partitioned].read(tempPath)
  }

}
