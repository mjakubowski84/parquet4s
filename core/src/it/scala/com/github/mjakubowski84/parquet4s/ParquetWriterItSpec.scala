package com.github.mjakubowski84.parquet4s

import java.nio.file.Files

import org.apache.parquet.hadoop.ParquetFileWriter
import org.scalatest.{BeforeAndAfter, FreeSpec, Matchers}

import scala.util.Random

class ParquetWriterItSpec
  extends FreeSpec
    with Matchers
    with BeforeAndAfter {

  case class Record(i: Int, d: Double, s: String)
  object Record {
    def random(n: Int): Seq[Record] =
      (1 to n).map(_ =>
        Record(Random.nextInt(), Random.nextDouble(), Random.nextString(10)))
  }

  private val tempDir = com.google.common.io.Files.createTempDir().toPath.toAbsolutePath
  private val writePath = tempDir.resolve("file.parquet")

  // Generate records and do a single batch write.
  private val records = Record.random(5000)

  private def readRecords: Seq[Record] = {
    val iter = ParquetReader.read[Record](writePath.toString)
    try iter.toSeq
    finally iter.close()
  }

  after { // Delete written files
    Files.deleteIfExists(writePath)
  }

  "Batch write should result in proper number of records in the file" in {
    ParquetWriter.writeAndClose(writePath.toString, records)
    readRecords should be(records)
  }

  "Multiple incremental writes produce same result as a single batch write" in {
    val w = ParquetWriter.writer[Record](writePath.toString)
    try records.grouped(5).foreach(w.write)
    finally w.close()
    readRecords shouldBe records
  }

  "Writing record by record works as well" in {
    val w = ParquetWriter.writer[Record](writePath.toString)
    try records.foreach(record => w.write(record))
    finally w.close()
    readRecords shouldBe records
  }

  "Incremental writes work with write mode OVERWRITE" in {
    val w = ParquetWriter.writer[Record](
      writePath.toString,
      ParquetWriter.Options(ParquetFileWriter.Mode.OVERWRITE))
    try records.grouped(5).foreach(w.write)
    finally w.close()
    readRecords shouldBe records
  }

  "Writing to closed writer throws an exception" in {
    val w = ParquetWriter.writer[Record](writePath.toString)
    w.close()
    an[IllegalStateException] should be thrownBy records
      .grouped(2)
      .foreach(w.write)
  }

  "Closing writer without writing anything to it throws no exception" in {
    val w = ParquetWriter.writer[Record](writePath.toString)
    noException should be thrownBy w.close()
  }

  "Closing writer twice throws no exception" in {
    val w = ParquetWriter.writer[Record](writePath.toString)
    noException should be thrownBy w.close()
    noException should be thrownBy w.close()
  }

}
