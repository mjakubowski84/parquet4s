package com.github.mjakubowski84.parquet4s

import java.nio.file.Files

import org.apache.hadoop.conf.Configuration
import org.apache.parquet.hadoop.util.HadoopInputFile
import org.apache.parquet.hadoop.{ParquetFileReader, ParquetFileWriter}
import org.scalatest.{BeforeAndAfter, Entry}
import org.scalatest.freespec.AnyFreeSpec
import org.scalatest.matchers.should.Matchers

import scala.util.Random

class ParquetWriterItSpec
  extends AnyFreeSpec
    with Matchers
    with BeforeAndAfter {

  case class Record(i: Int, d: Double, s: String)
  object Record {
    def random(n: Int): Seq[Record] =
      (1 to n).map(_ =>
        Record(Random.nextInt(), Random.nextDouble(), Random.nextString(10)))
  }

  private val tempDir = Path(Files.createTempDirectory("example"))
  private val writePath = tempDir.append("file.parquet")

  // Generate records and do a single batch write.
  private val records = Record.random(5000)

  private def readRecords: Seq[Record] = {
    val iter = ParquetReader.as[Record].read(writePath)
    try iter.toSeq
    finally iter.close()
  }

  after { // Delete written files
    Files.deleteIfExists(writePath.toNio)
  }

  "Batch write should result in proper number of records in the file" in {
    ParquetWriter.of[Record].writeAndClose(writePath, records)
    readRecords should be(records)
  }

  "Multiple incremental writes produce same result as a single batch write" in {
    val w = ParquetWriter.of[Record].build(writePath)
    try records.grouped(5).foreach(w.write)
    finally w.close()
    readRecords shouldBe records
  }

  "Writing record by record works as well" in {
    val w = ParquetWriter.of[Record].build(writePath)
    try records.foreach(record => w.write(record))
    finally w.close()
    readRecords shouldBe records
  }

  "Incremental writes work with write mode OVERWRITE" in {
    val w = ParquetWriter
      .of[Record]
      .options(ParquetWriter.Options(ParquetFileWriter.Mode.OVERWRITE))
      .build(writePath)
    try records.grouped(5).foreach(w.write)
    finally w.close()
    readRecords shouldBe records
  }

  "Writing to closed writer throws an exception" in {
    val w = ParquetWriter.of[Record].build(writePath)
    w.close()
    an[IllegalStateException] should be thrownBy records
      .grouped(2)
      .foreach(w.write)
  }

  "Closing writer without writing anything to it throws no exception" in {
    val w = ParquetWriter.of[Record].build(writePath)
    noException should be thrownBy w.close()
  }

  "Closing writer twice throws no exception" in {
    val w = ParquetWriter.of[Record].build(writePath)
    noException should be thrownBy w.close()
    noException should be thrownBy w.close()
  }

  "ParquetWriter should add metadata with Parquet4S signature" in {
    ParquetWriter.of[Record].writeAndClose(writePath, records)
    val meta = ParquetFileReader.open(
      HadoopInputFile.fromPath(writePath.toHadoop, new Configuration())
    ).getFileMetaData.getKeyValueMetaData
    meta should contain (Entry("MadeBy", "https://github.com/mjakubowski84/parquet4s"))
  }

}
