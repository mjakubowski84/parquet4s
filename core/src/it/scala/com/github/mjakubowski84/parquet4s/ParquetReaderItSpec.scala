package com.github.mjakubowski84.parquet4s

import com.github.mjakubowski84.parquet4s.ValueImplicits.*
import org.scalatest.BeforeAndAfter
import org.scalatest.freespec.AnyFreeSpec
import org.scalatest.matchers.should.Matchers

class ParquetReaderItSpec extends AnyFreeSpec with Matchers with TestUtils with BeforeAndAfter {

  case class Partitioned(a: String, b: String, i: Int)
  case class I(i: Int)

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
    try readRecords.toSeq should be(
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

    val partitioned = ParquetReader.as[Partitioned].partitioned.read(tempPath)
    try partitioned.toSeq should contain theSameElementsAs
      Seq(
        Partitioned(a = "a1", b = "b1", i = 1),
        Partitioned(a = "a1", b = "b2", i = 2),
        Partitioned(a = "a2", b = "b1", i = 3),
        Partitioned(a = "a2", b = "b2", i = 4)
      )
    finally partitioned.close()
  }

  "Partitions should be filtered" in {
    ParquetWriter.of[I].writeAndClose(Path(tempPath, "a=a1/b=b1/file.parquet"), Seq(I(1)))
    ParquetWriter.of[I].writeAndClose(Path(tempPath, "a=a1/b=b2/file.parquet"), Seq(I(2)))
    ParquetWriter.of[I].writeAndClose(Path(tempPath, "a=a2/b=b1/file.parquet"), Seq(I(3)))
    ParquetWriter.of[I].writeAndClose(Path(tempPath, "a=a2/b=b2/file.parquet"), Seq(I(4)))

    val partitioned = ParquetReader.as[Partitioned].partitioned.filter(Col("b") === "b1").read(tempPath)
    try partitioned.toSeq should contain theSameElementsAs
      Seq(
        Partitioned(a = "a1", b = "b1", i = 1),
        Partitioned(a = "a2", b = "b1", i = 3)
      )
    finally partitioned.close()
  }

  "Attempt to read partitions on non-partitioned data should succeed" in {
    ParquetWriter.of[I].writeAndClose(Path(tempPath, "file.parquet"), Seq(I(1), I(2)))

    val data = ParquetReader.as[I].partitioned.read(tempPath)
    try data.toSeq should be(Seq(I(1), I(2)))
    finally data.close()
  }

  "Attempt to read inconsistent partitions should fail with an exception" in {
    ParquetWriter.of[I].writeAndClose(Path(tempPath, "a=a1/b=b1/file.parquet"), Seq(I(1)))
    ParquetWriter.of[I].writeAndClose(Path(tempPath, "a=a2/file.parquet"), Seq(I(2)))

    an[IllegalArgumentException] should be thrownBy ParquetReader.as[Partitioned].partitioned.read(tempPath)
  }

}
