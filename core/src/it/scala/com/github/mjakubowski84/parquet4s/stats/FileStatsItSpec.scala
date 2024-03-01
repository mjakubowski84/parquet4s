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
import org.apache.parquet.io.InputFile
import scala.collection.compat.immutable.LazyList
import scala.annotation.unused

class FileStatsItSpec extends AnyFlatSpec with Matchers with BeforeAndAfterAll with Inspectors {

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

  implicit private val localDateOrdering: Ordering[LocalDate] = new Ordering[LocalDate] {
    override def compare(x: LocalDate, y: LocalDate): Int = x.compareTo(y)
  }

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

  "recordCound" should "return a valid number of records" in {
    Stats.builder.stats(inputFile).recordCount should be(dataSize)
  }

  it should "return 0 for an empty file" in {
    Stats.builder.stats(emptyInputFile).recordCount should be(0L)
  }

  "min and max" should "should return a valid value for each column" in {
    val maxIdx = dataSize - 1
    val stats  = Stats.builder.stats(inputFile)

    stats.min[Int](Col("idx")) should be(Some(0))
    stats.max[Int](Col("idx")) should be(Some(maxIdx))

    stats.min[Float](Col("float")) should be(Some(0.0f))
    stats.max[Float](Col("float")) should be(Some(maxIdx * 0.01f))

    stats.min[Double](Col("double")) should be(Some(0.0d))
    stats.max[Double](Col("double")) should be(Some(maxIdx * 0.00000001d))

    stats.min[String](Col("enum")) should be(Some("a"))
    stats.max[String](Col("enum")) should be(Some("d"))

    stats.min[Boolean](Col("bool")) should be(Some(false))
    stats.max[Boolean](Col("bool")) should be(Some(true))

    stats.min[LocalDate](Col("date")) should be(Some(zeroDate))
    stats.max[LocalDate](Col("date")) should be(Some(zeroDate.plusDays(maxIdx.toLong)))

    stats.min[BigDecimal](Col("decimal")) should be(Some(decimal(0)))
    stats.max[BigDecimal](Col("decimal")) should be(Some(decimal(maxIdx)))

    stats.min[Int](Col("embedded.x")) should be(Some(0))
    stats.max[Int](Col("embedded.x")) should be(Some(maxIdx))

    stats.min[Int](Col("optional")) should be(Some(1))
    stats.max[Int](Col("optional")) should be(Some(maxIdx))

    stats.min[Int](Col("invalid")) should be(None)
    stats.max[Int](Col("invalid")) should be(None)
  }

  it should "support projection" in {
    case class Projection(@unused idx: Int)
    val stats = Stats.builder.projection[Projection].stats(inputFile)
    stats.min[Int](Col("idx")) should be(Some(0))
    stats.max[Int](Col("idx")) should be(Some(dataSize - 1))
  }

  it should "return None for an empty file" in {
    val stats = Stats.builder.stats(emptyInputFile)

    stats.min[Int](Col("idx")) should be(None)
    stats.max[Int](Col("idx")) should be(None)
  }

}
