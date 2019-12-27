package com.github.mjakubowski84.parquet4s

import java.nio.file.Paths
import java.time.LocalDate

import com.google.common.io.Files
import org.apache.parquet.filter2.predicate.Operators.{Column, DoubleColumn, FloatColumn, SupportsLtGt}
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Inspectors, Matchers}

import scala.util.Random

class FilteringSpec extends FlatSpec with Matchers with BeforeAndAfterAll with Inspectors {


  case class Embedded(x: Int)
  case class Data(
                   idx: Int,
                   float: Float,
                   double: Double,
                   enum: String,
                   flag: Boolean,
                   date: LocalDate,
                   decimal: BigDecimal,
                   embedded: Embedded
                 )

  val enum: Seq[String] = List("a", "b", "c", "d")
  val dataSize: Int = 4096
  val halfSize: Int = dataSize / 2
  val filePath: String = Paths.get(Files.createTempDir().getAbsolutePath, "file.parquet").toString
  val zeroDate: LocalDate = LocalDate.of(1900, 1, 1)

  implicit val localDateOrdering: Ordering[LocalDate] = new Ordering[LocalDate] {
    override def compare(x: LocalDate, y: LocalDate): Int = x.compareTo(y)
  }

  def data: Stream[Data] =
    Stream.range(0, dataSize).map { i =>
      Data(
        idx = i,
        float = (BigDecimal("0.01") * BigDecimal(i)).toFloat,
        double = (BigDecimal("0.00000001") * BigDecimal(i)).toDouble,
        enum = enum(Random.nextInt(enum.size - 1)),
        flag = Random.nextBoolean(),
        date = zeroDate.plusDays(i),
        decimal = BigDecimal.valueOf(0.001 * (i - halfSize)),
        embedded = Embedded(i)
      )
    }

  override def beforeAll(): Unit = {
    super.beforeAll()
    ParquetWriter.writeAndClose(filePath, data, ParquetWriter.Options(
      rowGroupSize = 512 * 1024,
      pageSize = 128 * 1024,
      dictionaryPageSize = 128 * 1024
    ))
  }

  def read(filter: Filter): Seq[Data] = {
    val iter = ParquetReader.read[Data](filePath, filter = filter)
    try iter.toSeq
    finally iter.close()
  }

  def ltGtTest[T : Ordering, V <: Comparable[V], C <: Column[V] with SupportsLtGt](columnName: String, boundaryValue: T, field: Data => T)
                                                                                  (implicit filterValueConverter: FilterValueConverter[T, V, C]): Unit = {
    forExactly(halfSize, read(Col(columnName) < boundaryValue)) { dataRecord =>
      field(dataRecord) should be < boundaryValue
    }

    forExactly(halfSize + 1, read(Col(columnName) <= boundaryValue)) { dataRecord =>
      field(dataRecord) should be <= boundaryValue
    }

    field(read(Col(columnName) === boundaryValue).head) should be(boundaryValue)

    forExactly(halfSize, read(Col(columnName) >= boundaryValue)) { dataRecord =>
      field(dataRecord) should be >= boundaryValue
    }

    forExactly(halfSize - 1, read(Col(columnName) > boundaryValue)) { dataRecord =>
      field(dataRecord) should be > boundaryValue
    }
  }

  "Filtering" should "filter data by int" in ltGtTest("idx", halfSize, _.idx)

  it should "filter data by float" in ltGtTest[Float, java.lang.Float, FloatColumn]("float", 0.01f * halfSize, _.float)

  it should "filter data by double" in ltGtTest[Double, java.lang.Double, DoubleColumn]("double", 0.00000001d * halfSize, _.double)

  it should "filter data by text" in {
    val boundaryValue = "c"

    forAll(read(Col("enum") < boundaryValue)) { dataRecord =>
      dataRecord.enum should be < boundaryValue
    }

    forAll(read(Col("enum") <= boundaryValue)) { dataRecord =>
      dataRecord.enum should be <= boundaryValue
    }

    read(Col("enum") === boundaryValue).head.enum should be(boundaryValue)

    forAll(read(Col("enum") >= boundaryValue)) { dataRecord =>
      dataRecord.enum should be >= boundaryValue
    }

    forAll(read(Col("enum") > boundaryValue)) { dataRecord =>
      dataRecord.enum should be > boundaryValue
    }
  }

  it should "filter data by boolean" in {
    val trueData = read(Col("flag") === true)
    trueData.headOption should be (defined)
    forAll(trueData) { _.flag should be(true) }

    val falseData = read(Col("flag") === false)
    falseData.headOption should be (defined)
    forAll(falseData) { _.flag should be(false) }
  }

  it should "filter data by date" in ltGtTest("date", zeroDate.plusDays(halfSize), _.date)

  it should "filter data by decimal" in ltGtTest("decimal", BigDecimal(0), _.decimal)

  it should "filter data by embedded value" in ltGtTest("embedded.x", halfSize, _.idx)

  it should "leave data unfiltered when using noop filter" in {
    val filtered = read(Filter.noopFilter)

    filtered should have size dataSize
  }

}
