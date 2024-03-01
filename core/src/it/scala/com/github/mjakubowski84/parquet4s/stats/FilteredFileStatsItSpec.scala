package com.github.mjakubowski84.parquet4s.stats

import java.time.LocalDate
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.BeforeAndAfterAll
import org.scalatest.Inspectors
import scala.util.Random
import java.util.UUID
import com.github.mjakubowski84.parquet4s.ParquetWriter
import com.github.mjakubowski84.parquet4s.InMemoryOutputFile
import com.github.mjakubowski84.parquet4s.Stats
import com.github.mjakubowski84.parquet4s.Col
import com.github.mjakubowski84.parquet4s.Filter
import org.apache.parquet.io.InputFile
import scala.collection.compat.immutable.LazyList

class FilteredFileStatsItSpec extends AnyFlatSpec with Matchers with BeforeAndAfterAll with Inspectors {

  private case class Embedded(x: Int)
  private case class Data(
      idx: Int,
      float: Float,
      double: Double,
      `enum`: String,
      bool: Boolean,
      date: LocalDate,
      decimal: BigDecimal,
      embedded: Embedded,
      optional: Option[Int],
      random: String
  )

  private val `enum`: Seq[String]         = List("a", "b", "c", "d")
  private val dataSize: Int               = 256 * 256
  private val halfSize: Int               = dataSize / 2
  private val zeroDate: LocalDate         = LocalDate.of(1900, 1, 1)
  private def decimal(i: Int): BigDecimal = BigDecimal.valueOf(0.001 * (i - halfSize))

  private lazy val data: LazyList[Data] =
    LazyList.range(0, dataSize).map { i =>
      Data(
        idx      = i,
        float    = (BigDecimal("0.01") * BigDecimal(i)).toFloat,
        double   = (BigDecimal("0.00000001") * BigDecimal(i)).toDouble,
        `enum`   = `enum`(Random.nextInt(`enum`.size)),
        bool     = Random.nextBoolean(),
        date     = zeroDate.plusDays(i.toLong),
        decimal  = decimal(i),
        embedded = Embedded(i),
        optional = if (i % 2 == 0) None else Some(i),
        random   = UUID.randomUUID().toString
      )
    }

  private val filterByRandom: Filter                = Col("random") >= "a" && Col("random") <= "b"
  private lazy val filteredByRandom: LazyList[Data] = data.filter(v => v.random >= "a" && v.random <= "b")

  private val writeOptions = ParquetWriter.Options(
    rowGroupSize       = dataSize.toLong / 16L,
    pageSize           = 16 * 256,
    dictionaryPageSize = 16 * 256
  )

  private var inputFile: InputFile      = null
  private var emptyInputFile: InputFile = null

  override def beforeAll(): Unit = {
    super.beforeAll()
    val outputFile = InMemoryOutputFile(initBufferSize = 256 * dataSize)
    ParquetWriter.of[Data].options(writeOptions).writeAndClose(outputFile, data)
    inputFile = outputFile.toInputFile

    val emptyOutputFile = InMemoryOutputFile(initBufferSize = 256)
    ParquetWriter.of[Data].options(writeOptions).writeAndClose(emptyOutputFile, Seq.empty)
    emptyInputFile = emptyOutputFile.toInputFile
  }

  "recordCound" should "be valid when filtering an interval out of monotonic value " in {
    val lowerBound   = 16
    val upperBound   = 116
    val expectedSize = upperBound - lowerBound
    Stats.builder.filter(Col("idx") > lowerBound && Col("idx") <= upperBound).stats(inputFile).recordCount should be(
      expectedSize
    )
  }

  it should "be valid when filtering over random data" in {
    val expectedSize = filteredByRandom.size
    Stats.builder.filter(filterByRandom).stats(inputFile).recordCount should be(expectedSize)
  }

  it should "be valid when filtering with not eq" in {
    val expectedSize = dataSize - 1
    Stats.builder.filter(Col("idx") !== 200).stats(inputFile).recordCount should be(expectedSize)
  }

  it should "return 0 for an empty file" in {
    Stats.builder.filter(filterByRandom).stats(emptyInputFile).recordCount should be(0L)
  }

  "min & max" should "provide proper value when filtering" in {
    val expectedMin = 16
    val expectedMax = 128
    val stats       = Stats.builder.filter(Col("idx") >= expectedMin && Col("idx") <= expectedMax).stats(inputFile)
    stats.min[Int](Col("idx")) should be(Some(expectedMin))
    stats.max[Int](Col("idx")) should be(Some(expectedMax))
  }

  it should "be valid when filtering over random data" in {
    val expectedMax = Option(filteredByRandom.maxBy(_.random).random)
    val expectedMin = Option(filteredByRandom.minBy(_.random).random)

    val stats = Stats.builder.filter(filterByRandom).stats(inputFile)
    stats.max[String](Col("random")) should be(expectedMax)
    stats.min[String](Col("random")) should be(expectedMin)
  }

  it should "provide proper value for an empty file" in {
    val stats = Stats.builder.filter(Col("idx") >= 1 && Col("idx") <= 10).stats(emptyInputFile)
    stats.min[Int](Col("idx")) should be(None)
    stats.max[Int](Col("idx")) should be(None)
  }

}
