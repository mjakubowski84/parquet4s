package com.github.mjakubowski84.parquet4s

import com.github.mjakubowski84.parquet4s.ValueImplicits.*
import org.scalatest.BeforeAndAfter
import org.scalatest.freespec.AnyFreeSpec
import org.scalatest.matchers.should.Matchers

import java.nio.file.Files

class ParquetReaderItSpec
  extends AnyFreeSpec
    with Matchers
    with BeforeAndAfter {

  case class Record(i: Int, d: Option[Double], nested: Nested)
  case class Nested(s: String)

  private val tempDir = Path(Files.createTempDirectory("example"))
  private val path = tempDir.append("file.parquet")

  after { // Delete written files
    Files.deleteIfExists(path.toNio)
  }

  "Reading generic records from schema containing optional fields should result in NullValues for missing data" in {
    val records =
      Seq(
        Record(1, Some(2.1), Nested("non-null")),
        Record(1, None, Nested(null))
      )
    ParquetWriter.of[Record].writeAndClose(path, records)
    val readRecords = ParquetReader.generic.read(path)

    try {
      readRecords.toSeq should be(Seq(
        RowParquetRecord("i" -> 1.value, "d" -> 2.1.value, "nested" -> RowParquetRecord("s" -> "non-null".value)),
        RowParquetRecord("i" -> 1.value, "d" -> NullValue, "nested" -> RowParquetRecord("s" -> NullValue))
      ))
    } finally {
      readRecords.close()
    }
  }


}
