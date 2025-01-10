package com.github.mjakubowski84.parquet4s

import java.nio.file.Files
import java.time.{LocalDate, LocalDateTime, LocalTime}
import org.apache.parquet.filter2.predicate.Operators.{Column, DoubleColumn, FloatColumn, SupportsLtGt}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.{Assertion, BeforeAndAfterAll, Inspectors}

import scala.util.Random
import scala.collection.compat.*
import immutable.LazyList

class FilteringItSpec extends AnyFlatSpec with Matchers with BeforeAndAfterAll with Inspectors {

  case class Embedded(x: Int)
  case class Data(
      idx: Int,
      float: Float,
      double: Double,
      `enum`: String,
      flag: Boolean,
      date: LocalDate,
      dateTime: LocalDateTime,
      decimal: BigDecimal,
      embedded: Embedded,
      nullable: Option[Int]
  )

  val `enum`: Seq[String]         = List("a", "b", "c", "d")
  val dataSize: Int               = 4096
  val halfSize: Int               = dataSize / 2
  val filePath: Path              = Path(Path(Files.createTempDirectory("example")), "file.parquet")
  val millisFilePath: Path        = Path(Path(Files.createTempDirectory("example")), "millis.parquet")
  val microsFilePath: Path        = Path(Path(Files.createTempDirectory("example")), "micros.parquet")
  val nanosFilePath: Path         = Path(Path(Files.createTempDirectory("example")), "nanos.parquet")
  val zeroDate: LocalDate         = LocalDate.of(1900, 1, 1)
  val zeroDateTime: LocalDateTime = LocalDateTime.of(zeroDate, LocalTime.ofNanoOfDay(0))

  implicit val localDateOrdering: Ordering[LocalDate] = new Ordering[LocalDate] {
    override def compare(x: LocalDate, y: LocalDate): Int = x.compareTo(y)
  }

  implicit val localDateTimeOrdering: Ordering[LocalDateTime] = new Ordering[LocalDateTime] {
    override def compare(x: LocalDateTime, y: LocalDateTime): Int = x.compareTo(y)
  }

  def data: LazyList[Data] =
    LazyList.range(0, dataSize).map { i =>
      Data(
        idx      = i,
        float    = (BigDecimal("0.01") * BigDecimal(i)).toFloat,
        double   = (BigDecimal("0.00000001") * BigDecimal(i)).toDouble,
        `enum`   = `enum`(Random.nextInt(`enum`.size)),
        flag     = Random.nextBoolean(),
        date     = zeroDate.plusDays(i.toLong),
        dateTime = zeroDateTime.plusSeconds(i.toLong),
        decimal  = BigDecimal.valueOf(0.001 * (i - halfSize)),
        embedded = Embedded(i),
        nullable = if (i % 2 == 0) None else Some(i)
      )
    }

  override def beforeAll(): Unit = {
    super.beforeAll()
    val options = ParquetWriter.Options(
      rowGroupSize       = 512 * 1024,
      pageSize           = 128 * 1024,
      dictionaryPageSize = 128 * 1024
    )
    locally {
      ParquetWriter
        .of[Data]
        .options(options)
        .writeAndClose(filePath, data)
    }
    locally {
      import TimestampFormat.Implicits.Millis.*
      ParquetWriter
        .of[Data]
        .options(options)
        .writeAndClose(millisFilePath, data)
    }
    locally {
      import TimestampFormat.Implicits.Micros.*
      ParquetWriter
        .of[Data]
        .options(options)
        .writeAndClose(microsFilePath, data)
    }
    locally {
      import TimestampFormat.Implicits.Nanos.*
      ParquetWriter
        .of[Data]
        .options(options)
        .writeAndClose(nanosFilePath, data)
    }
  }

  def read(filter: Filter, path: Path = filePath): Seq[Data] = {
    val iter = ParquetReader.as[Data].filter(filter).read(path)
    try iter.toSeq
    finally iter.close()
  }

  def ltGtTest[T: Ordering, V <: Comparable[V], C <: Column[V] & SupportsLtGt](
      columnName: String,
      boundaryValue: T,
      field: Data => T,
      path: Path = filePath
  )(implicit codec: FilterCodec[T, V, C]): Assertion = {
    forExactly(halfSize, read(Col(columnName) < boundaryValue, path)) { dataRecord =>
      field(dataRecord) should be < boundaryValue
    }

    forExactly(halfSize + 1, read(Col(columnName) <= boundaryValue, path)) { dataRecord =>
      field(dataRecord) should be <= boundaryValue
    }

    field(read(Col(columnName) === boundaryValue, path).head) should be(boundaryValue)

    forExactly(halfSize, read(Col(columnName) >= boundaryValue, path)) { dataRecord =>
      field(dataRecord) should be >= boundaryValue
    }

    forExactly(halfSize - 1, read(Col(columnName) > boundaryValue, path)) { dataRecord =>
      field(dataRecord) should be > boundaryValue
    }
  }

  "Filtering" should "filter data by int" in ltGtTest("idx", halfSize, _.idx)

  it should "filter data by float" in ltGtTest[Float, java.lang.Float, FloatColumn]("float", 0.01f * halfSize, _.float)

  it should "filter data by double" in ltGtTest[Double, java.lang.Double, DoubleColumn](
    "double",
    0.00000001d * halfSize,
    _.double
  )

  it should "filter data by text" in {
    val boundaryValue = "c"

    forAll(read(Col("enum") < boundaryValue)) { dataRecord =>
      dataRecord.`enum` should be < boundaryValue
    }

    forAll(read(Col("enum") <= boundaryValue)) { dataRecord =>
      dataRecord.`enum` should be <= boundaryValue
    }

    read(Col("enum") === boundaryValue).head.`enum` should be(boundaryValue)

    forAll(read(Col("enum") >= boundaryValue)) { dataRecord =>
      dataRecord.`enum` should be >= boundaryValue
    }

    forAll(read(Col("enum") > boundaryValue)) { dataRecord =>
      dataRecord.`enum` should be > boundaryValue
    }
  }

  it should "filter data by boolean" in {
    val trueData = read(Col("flag") === true)
    trueData.headOption should be(defined)
    forAll(trueData)(_.flag should be(true))

    val falseData = read(Col("flag") === false)
    falseData.headOption should be(defined)
    forAll(falseData)(_.flag should be(false))
  }

  it should "filter data by date" in ltGtTest("date", zeroDate.plusDays(halfSize.toLong), _.date)

  it should "filter data by dateTime INT64 millis" in {
    import TimestampFormat.Implicits.Millis.*
    ltGtTest("dateTime", zeroDateTime.plusSeconds(halfSize.toLong), _.dateTime, millisFilePath)
  }

  it should "filter data by dateTime INT64 micros" in {
    import TimestampFormat.Implicits.Micros.*
    ltGtTest("dateTime", zeroDateTime.plusSeconds(halfSize.toLong), _.dateTime, microsFilePath)
  }

  it should "filter data by dateTime INT64 nanos" in {
    import TimestampFormat.Implicits.Nanos.*
    ltGtTest("dateTime", zeroDateTime.plusSeconds(halfSize.toLong), _.dateTime, nanosFilePath)
  }

  it should "filter data by decimal" in ltGtTest("decimal", BigDecimal(0), _.decimal)

  it should "filter data by embedded value" in ltGtTest("embedded.x", halfSize, _.idx)

  it should "leave data unfiltered when using noop filter" in {
    val filtered = read(Filter.noopFilter)

    filtered should have size dataSize.toLong
  }

  it should "filter by udp" in {
    object IntDividesBy10 extends UDP[Int] {
      private val Ten                        = 10
      override def keep(value: Int): Boolean = value % Ten == 0
      @inline
      override def canDrop(statistics: FilterStatistics[Int]): Boolean = {
        val minMod = statistics.min % Ten
        val maxMod = statistics.max % Ten
        (statistics.max - statistics.min < Ten) && maxMod >= minMod
      }
      override def inverseCanDrop(statistics: FilterStatistics[Int]): Boolean = !canDrop(statistics)
      override val name: String                                               = "IntDividesBy10"
    }

    forExactly((dataSize / 10) + 1, read(Col("idx").udp(IntDividesBy10))) { dataRecord =>
      dataRecord.idx % 10 should be(0)
    }
  }

  it should "filter data by null values" in {
    val filteredEmpty = read(Col("enum").isNull[String])
    filteredEmpty should be(empty)
    val filteredFull = read(Col("enum").isNotNull[String])
    filteredFull should have size dataSize.toLong

    val filteredSomeEmpty = read(Col("nullable").isNull[Int])
    filteredSomeEmpty should have size dataSize.toLong / 2

    val filteredSomeFull = read(Col("nullable").isNotNull[Int])
    filteredSomeFull should have size dataSize.toLong / 2
  }

}
