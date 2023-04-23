package com.github.mjakubowski84.parquet4s

import org.apache.parquet.filter2.predicate.Operators.{Column, DoubleColumn, FloatColumn, SupportsEqNotEq}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.{Assertion, BeforeAndAfterAll, Inspectors}

import java.nio.file.Files
import java.sql.Date
import java.time.LocalDate
import scala.collection.compat.immutable.LazyList
import scala.util.Random

class FilteringByListSpec extends AnyFlatSpec with Matchers with BeforeAndAfterAll with Inspectors {

  case class Embedded(x: Int)

  case class Data(
      idx: Int,
      short: Short,
      byte: Byte,
      char: Char,
      long: Long,
      float: Float,
      double: Double,
      `enum`: String,
      flag: Boolean,
      date: LocalDate,
      sqlDate: Date,
      decimal: BigDecimal,
      embedded: Embedded
  )

  val `enum`: Seq[String] = List("a", "b", "c", "d")
  val dataSize: Int       = 4096
  val halfSize: Int       = dataSize / 2
  val filePath: Path      = Path(Path(Files.createTempDirectory("example")), "file.parquet")
  val zeroDate: LocalDate = LocalDate.of(1900, 1, 1)

  implicit val localDateOrdering: Ordering[LocalDate] = new Ordering[LocalDate] {
    override def compare(x: LocalDate, y: LocalDate): Int = x.compareTo(y)
  }

  implicit val sqlDateOrdering: Ordering[Date] = new Ordering[Date] {
    override def compare(x: Date, y: Date): Int = x.compareTo(y)
  }

  def data: LazyList[Data] =
    LazyList.range(0, dataSize).map { i =>
      Data(
        idx      = i,
        short    = (i % Short.MaxValue).toShort,
        byte     = (i % Byte.MaxValue).toByte,
        char     = (i % Char.MaxValue).toChar,
        long     = i.toLong,
        float    = (BigDecimal("0.01") * BigDecimal(i)).toFloat,
        double   = (BigDecimal("0.00000001") * BigDecimal(i)).toDouble,
        `enum`   = `enum`(Random.nextInt(`enum`.size)),
        flag     = Random.nextBoolean(),
        date     = zeroDate.plusDays(i),
        sqlDate  = Date.valueOf(zeroDate.plusDays(i)),
        decimal  = BigDecimal.valueOf(0.001 * (i - halfSize)),
        embedded = Embedded(i)
      )
    }

  def everyOtherDatum: LazyList[Data] = data.filter(_.idx % 2 == 0)

  override def beforeAll(): Unit = {
    super.beforeAll()
    ParquetWriter
      .of[Data]
      .options(
        ParquetWriter.Options(
          rowGroupSize       = 512 * 1024,
          pageSize           = 128 * 1024,
          dictionaryPageSize = 128 * 1024
        )
      )
      .writeAndClose(filePath, data)
  }

  def genericFilterTest[T: Ordering, V <: Comparable[V], C <: Column[V] & SupportsEqNotEq](
      columnName: String,
      field: Data => T
  )(implicit codec: FilterCodec[T, V, C]): Assertion = {
    val actual = ParquetReader.as[Data].filter(Col(columnName) in everyOtherDatum.map(field)).read(filePath)
    try actual.map(_.idx) should equal(everyOtherDatum.map(_.idx))
    finally actual.close()
  }

  def specificValueFilterTest[T: Ordering, V <: Comparable[V], C <: Column[V] & SupportsEqNotEq](
      columnName: String,
      field: Data => T,
      values: Vector[T]
  )(implicit codec: FilterCodec[T, V, C]): Assertion = {
    val filteredRecords   = ParquetReader.as[Data].filter(Col(columnName) in values).read(filePath)
    val unfilteredRecords = ParquetReader.as[Data].read(filePath)

    try {
      filteredRecords.map(field).toSet should contain theSameElementsAs values
      val manuallyFilteredRecords = unfilteredRecords.filter(row => values.contains(field(row)))
      filteredRecords.map(_.idx) should equal(manuallyFilteredRecords.map(_.idx))
    } finally {
      filteredRecords.close()
      unfilteredRecords.close()
    }

  }

  "Filtering" should "filter data by a list of ints" in genericFilterTest("idx", _.idx)

  it should "filter data by a list of shorts" in genericFilterTest("short", _.short)

  it should "filter data by a list of bytes" in specificValueFilterTest(
    "byte",
    _.byte,
    Vector(0, 1, 2, 3).map(_.toByte)
  )

  it should "filter data by a list of chars" in genericFilterTest("char", _.char)

  it should "filter data by a list of longs" in genericFilterTest("long", _.long)

  it should "filter data by a list of floats" in genericFilterTest[Float, java.lang.Float, FloatColumn](
    "float",
    _.float
  )

  it should "filter data by a list of doubles" in genericFilterTest[Double, java.lang.Double, DoubleColumn](
    "double",
    _.double
  )

  it should "filter data by a list of strings" in specificValueFilterTest("enum", _.`enum`, Vector("a", "b"))

  it should "filter data by a list of SQL dates" in genericFilterTest("sqlDate", _.sqlDate)

  it should "filter data by a list of dates" in genericFilterTest("date", _.date)

  it should "filter data by a list of decimals" in genericFilterTest("decimal", _.decimal)

  it should "filter data by a list of embedded values" in genericFilterTest("embedded.x", _.idx)

  it should "filter data by a hard-coded list of values" in {
    val filteredRecords = ParquetReader.as[Data].filter(Col("idx").in(1, 2, 3)).read(filePath)
    try {
      filteredRecords.size should equal(3)
      filteredRecords.map(_.idx) should contain allOf (1, 2, 3)
    } finally filteredRecords.close()
  }

  it should "inverse filter data by a hard-coded list of values" in {
    val filteredRecords = ParquetReader.as[Data].filter(!Col("idx").in(1, 2, 3)).read(filePath)
    try {
      filteredRecords.toSeq.size should equal(dataSize - 3)
      filteredRecords.map(_.idx) should contain noneOf (1, 2, 3)
    } finally filteredRecords.close()
  }

  it should "reject an empty set of keys" in {
    a[IllegalArgumentException] should be thrownBy (Col("idx") in Set.empty[Int])
  }
}
