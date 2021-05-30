package com.github.mjakubowski84.parquet4s

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.{BeforeAndAfterAll, Inspectors}

import java.nio.file.Files
import java.time.LocalDate
import java.util.UUID
import scala.collection.compat.immutable.LazyList
import scala.util.Random

class StatsSpec extends AnyFlatSpec with Matchers with BeforeAndAfterAll with Inspectors {

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

  val `enum`: Seq[String] = List("a", "b", "c", "d")
  val dataSize: Int = 256 * 256
  val halfSize: Int = dataSize / 2
  val path: Path = Path(Files.createTempDirectory("example"))
  val zeroDate: LocalDate = LocalDate.of(1900, 1, 1)
  def decimal(i: Int): BigDecimal = BigDecimal.valueOf(0.001 * (i - halfSize))

  implicit val localDateOrdering: Ordering[LocalDate] = new Ordering[LocalDate] {
    override def compare(x: LocalDate, y: LocalDate): Int = x.compareTo(y)
  }
  val vcc: ValueCodecConfiguration = ValueCodecConfiguration.Default

  lazy val data: LazyList[Data] =
    LazyList.range(0, dataSize).map { i =>
      Data(
        idx = i,
        float = (BigDecimal("0.01") * BigDecimal(i)).toFloat,
        double = (BigDecimal("0.00000001") * BigDecimal(i)).toDouble,
        `enum` = `enum`(Random.nextInt(`enum`.size)),
        bool = Random.nextBoolean(),
        date = zeroDate.plusDays(i),
        decimal = decimal(i),
        embedded = Embedded(i),
        optional = if (i % 2 == 0) None else Some(i),
        random = UUID.randomUUID().toString
      )
    }
  val filterByRandom: Filter = Col("random") >= "a" && Col("random") <= "b"
  lazy val filteredByRandom: LazyList[Data] = data.filter(v => v.random >= "a" && v.random <= "b")

  override def beforeAll(): Unit = {
    super.beforeAll()
    val writeOptions = ParquetWriter.Options(
      rowGroupSize = dataSize / 16,
      pageSize = 16 * 256,
      dictionaryPageSize = 16 * 256
    )
    val window = dataSize / 4
    data
      .sliding(window, window)
      .zipWithIndex
      .foreach { case (part, index) =>
        ParquetWriter.writeAndClose(path.append(s"$index.parquet"), part, writeOptions)
      }

  }

  "recordCount" should "be valid for a single file" in {
    Stats(path.append("0.parquet")).recordCount should be(dataSize / 4)
  }

  it should "be valid for a whole dataset" in {
    Stats(path).recordCount should be(dataSize)
  }

  it should "be valid when filtering an interval out of monotonic value " in {
    val lowerBound = 16
    val upperBound = 116
    val expectedSize = upperBound - lowerBound
    Stats(path, filter = Col("idx") > lowerBound && Col("idx") <= upperBound).recordCount should be(expectedSize)
  }

  it should "be valid when filtering over random data" in {
    val expectedSize = filteredByRandom.size
    Stats(path, filter = filterByRandom).recordCount should be(expectedSize)
  }

  "min & max" should "should provide proper value for a single file" in {
    val stats = Stats(path.append("0.parquet"))
    stats.min[Int](Col("idx")) should be(Some(0))
    stats.max[Int](Col("idx")) should be(Some((dataSize / 4) - 1))
  }

  it should "should provide proper value for a single file when filtering" in {
    val expectedMin = 16
    val expectedMax = 128
    val stats = Stats(path.append("0.parquet"), filter = Col("idx") >= expectedMin && Col("idx") <= expectedMax)
    stats.min[Int](Col("idx")) should be(Some(expectedMin))
    stats.max[Int](Col("idx")) should be(Some(expectedMax))
  }

  it should "be valid when filtering over random data" in {
    val expectedMax = Option(filteredByRandom.maxBy(_.random).random)
    val expectedMin = Option(filteredByRandom.minBy(_.random).random)

    val stats = Stats(path, filter = filterByRandom)
    stats.max[String](Col("random")) should be(expectedMax)
    stats.min[String](Col("random")) should be(expectedMin)
  }

  it should "should provide proper value for each column" in {
    val maxIdx = dataSize - 1
    val stats = Stats(path)

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
    stats.max[LocalDate](Col("date")) should be(Some(zeroDate.plusDays(maxIdx)))

    stats.min[BigDecimal](Col("decimal")) should be(Some(decimal(0)))
    stats.max[BigDecimal](Col("decimal")) should be(Some(decimal(maxIdx)))

    stats.min[Int](Col("embedded.x")) should be(Some(0))
    stats.max[Int](Col("embedded.x")) should be(Some(maxIdx))

    stats.min[Int](Col("optional")) should be(Some(1))
    stats.max[Int](Col("optional")) should be(Some(maxIdx))

    stats.min[Int](Col("invalid")) should be(None)
    stats.max[Int](Col("invalid")) should be(None)
  }

}
