package com.github.mjakubowski84.parquet4s.stats

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.{BeforeAndAfterAll, Inspectors}

import java.nio.file.Files
import java.time.LocalDate
import java.util.UUID
import scala.collection.compat.immutable.LazyList
import scala.util.Random
import com.github.mjakubowski84.parquet4s.{Col, Filter, ParquetWriter, Path, Stats}

class CompoundAndPartitionedStatsITSpec extends AnyFlatSpec with Matchers with BeforeAndAfterAll with Inspectors {

  case class Embedded(x: Int)
  case class Data(
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

  val `enum`: Seq[String]         = List("a", "b", "c", "d")
  val dataSize: Int               = 256 * 256
  val halfSize: Int               = dataSize / 2
  val path: Path                  = Path(Files.createTempDirectory("example"))
  val zeroDate: LocalDate         = LocalDate.of(1900, 1, 1)
  def decimal(i: Int): BigDecimal = BigDecimal.valueOf(0.001 * (i - halfSize))

  implicit val localDateOrdering: Ordering[LocalDate] = new Ordering[LocalDate] {
    override def compare(x: LocalDate, y: LocalDate): Int = x.compareTo(y)
  }

  lazy val data: LazyList[Data] =
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
  val filterByRandom: Filter                = Col("random") >= "a" && Col("random") <= "b"
  lazy val filteredByRandom: LazyList[Data] = data.filter(v => v.random >= "a" && v.random <= "b")

  override def beforeAll(): Unit = {
    super.beforeAll()
    val writeOptions = ParquetWriter.Options(
      rowGroupSize       = dataSize.toLong / 16L,
      pageSize           = 16 * 256,
      dictionaryPageSize = 16 * 256
    )
    val window = dataSize / 4
    data
      .sliding(window, window)
      .zipWithIndex
      .foreach { case (part, index) =>
        ParquetWriter.of[Data].options(writeOptions).writeAndClose(path.append(s"partition=$index/file.parquet"), part)
      }
  }

  "recordCount" should "be valid for a single file" in {
    Stats.builder.filter(Col("partition") === "0").stats(path).recordCount should be(dataSize / 4)
  }

  it should "be valid for a whole dataset" in {
    Stats.builder.stats(path).recordCount should be(dataSize)
  }

  "min & max" should "should provide proper value for a single file" in {
    val stats = Stats.builder.filter(Col("partition") === "0").stats(path)
    stats.min[Int](Col("idx")) should be(Some(0))
    stats.max[Int](Col("idx")) should be(Some((dataSize / 4) - 1))
  }

  it should "should provide proper value when filtering whole dataset" in {
    val expectedMin = 16
    val expectedMax = 128
    val stats       = Stats.builder.filter(Col("idx") >= expectedMin && Col("idx") <= expectedMax).stats(path)
    stats.min[Int](Col("idx")) should be(Some(expectedMin))
    stats.max[Int](Col("idx")) should be(Some(expectedMax))
  }

  it should "should provide proper value when reading boundaries of partition values" in {
    val stats = Stats.builder.stats(path)
    stats.min[String](Col("partition")) should be(Some("0"))
    stats.max[String](Col("partition")) should be(Some("3"))
  }

}
