package com.github.mjakubowski84.parquet4s

import java.nio.file.{Files, Paths}

import org.apache.parquet.hadoop.ParquetFileWriter
import org.scalatest.{BeforeAndAfter, FreeSpec, Matchers}

import scala.util.Random

class IncrementalParquetWriterItSpec
    extends FreeSpec
    with Matchers
    with BeforeAndAfter {

  case class Record(i: Int, d: Double, s: String)
  object Record {
    def random(n: Int): Seq[Record] =
      (1 to n).map(_ =>
        Record(Random.nextInt(), Random.nextDouble(), Random.nextString(10)))
  }

  private val tempDir = com.google.common.io.Files.createTempDir().toPath
  private val batchPath = Paths.get(s"$tempDir/batch.parquet")
  private val incrementalPath = Paths.get(s"$tempDir/incremental.parquet")

  // Generate records and do a single batch write.
  private val records = Record.random(5000)
  ParquetWriter.write(batchPath.toString, records)

  private def incrementalRecords: Seq[Record] = {
    val iter = ParquetReader.read[Record](incrementalPath.toString)
    try iter.toSeq
    finally iter.close()
  }

  after { // Delete the incremental path after writing.
    Files.deleteIfExists(incrementalPath)
  }

  "Single incremental write produces the same result as a single batch write" in {
    val w = IncrementalParquetWriter[Record](incrementalPath.toString)
    try w.write(records)
    finally w.close()
    incrementalRecords shouldBe records
  }

  "Multiple incremental writes produce same result as a single batch write" in {
    val w = IncrementalParquetWriter[Record](incrementalPath.toString)
    try records.grouped(2).foreach(w.write)
    finally w.close()
    incrementalRecords shouldBe records
  }

  "Synchronized parallel writes produce same result (after sorting) as a single batch write" in {
    val w = IncrementalParquetWriter[Record](incrementalPath.toString)
    try records.grouped(2).toSeq.par.foreach(g => synchronized(w.write(g)))
    finally w.close()
    incrementalRecords.sortBy(_.toString) shouldBe records.sortBy(_.toString)
  }

  "Incremental writes work with write mode OVERWRITE" in {
    val w = IncrementalParquetWriter[Record](
      incrementalPath.toString,
      ParquetWriter.Options(ParquetFileWriter.Mode.OVERWRITE))
    try records.grouped(2).foreach(w.write)
    finally w.close()
    incrementalRecords shouldBe records
  }

  "Writing to closed writer throws an exception" in {
    val w = IncrementalParquetWriter[Record](incrementalPath.toString)
    w.close()
    an[IllegalStateException] should be thrownBy records
      .grouped(2)
      .foreach(w.write)
  }

  "Closing writer without writing anything to it throws no exception" in {
    val w = IncrementalParquetWriter[Record](incrementalPath.toString)
    noException should be thrownBy w.close()
  }

  "Closing writer twice throws no exception" in {
    val w = IncrementalParquetWriter[Record](incrementalPath.toString)
    noException should be thrownBy w.close()
    noException should be thrownBy w.close()
  }

}
