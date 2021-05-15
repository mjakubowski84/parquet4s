package com.github.mjakubowski84.parquet4s

import com.github.mjakubowski84.parquet4s.ParquetReader.newParquetIterable
import com.github.mjakubowski84.parquet4s.ValueImplicits._
import org.apache.parquet.hadoop.{ParquetReader => HadoopParquetReader}
import org.mockito.scalatest.IdiomaticMockito
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

object ParquetIterableSpec {

  case class TestRow(int: Int)

  def testRecord(int: Int): RowParquetRecord = RowParquetRecord("int" -> int)

  val vcc: ValueCodecConfiguration = ValueCodecConfiguration.default
}

class ParquetIterableSpec extends AnyFlatSpec with Matchers with IdiomaticMockito {

  import ParquetIterableSpec._

  private def mockTestBuilder(reader: HadoopParquetReader[RowParquetRecord]) = {
    val builder = mock[ParquetReader.Builder]
    builder.build() returns reader
    builder
  }

  "iterator" should "build instance of iterator over row class containing record reader" in {
    newParquetIterable[TestRow](
      mockTestBuilder(mock[HadoopParquetReader[RowParquetRecord]]),
      vcc,
      mock[Stats]
    ).iterator should be(an[Iterator[_]])
  }

  it should "build a new iterator with new reader every time called" in {
    val builder = mockTestBuilder(mock[HadoopParquetReader[RowParquetRecord]])

    newParquetIterable[TestRow](builder, vcc, mock[Stats]).iterator should be(an[Iterator[_]])
    newParquetIterable[TestRow](builder, vcc, mock[Stats]).iterator should be(an[Iterator[_]])
    newParquetIterable[TestRow](builder, vcc, mock[Stats]).iterator should be(an[Iterator[_]])

    builder.build() wasCalled 3.times
  }

  "hasNext" should "return false for empty resource" in {
    val reader = mock[HadoopParquetReader[RowParquetRecord]]
    reader.read() returns null

    newParquetIterable[TestRow](mockTestBuilder(reader), vcc, mock[Stats]).iterator.hasNext should be(false)

    reader.read() was called
  }

  it should "return true for single-record resource" in {
    val reader = mock[HadoopParquetReader[RowParquetRecord]]
    reader.read() returns testRecord(1)

    newParquetIterable[TestRow](mockTestBuilder(reader), vcc, mock[Stats]).iterator.hasNext should be(true)

    reader.read() was called
  }

  it should "call 'read' when it is called itself multiple times in sequence (and return false)" in {
    val reader = mock[HadoopParquetReader[RowParquetRecord]]
    reader.read() returns null

    val iterator = newParquetIterable[TestRow](mockTestBuilder(reader), vcc, mock[Stats]).iterator
    iterator.hasNext should be(false)
    iterator.hasNext should be(false)
    iterator.hasNext should be(false)

    reader.read() wasCalled once
  }

  it should "call 'read' when it is called itself multiple times in sequence (and return true)" in {
    val reader = mock[HadoopParquetReader[RowParquetRecord]]
    reader.read() returns testRecord(1)

    val iterator = newParquetIterable[TestRow](mockTestBuilder(reader), vcc, mock[Stats]).iterator
    iterator.hasNext should be(true)
    iterator.hasNext should be(true)
    iterator.hasNext should be(true)

    reader.read() wasCalled once
  }

  "next" should "return row for single-record resource" in {
    val reader = mock[HadoopParquetReader[RowParquetRecord]]
    reader.read() returns testRecord(1)

    newParquetIterable[TestRow](mockTestBuilder(reader), vcc, mock[Stats]).iterator.next() should be(TestRow(1))
  }

  it should "throw NoSuchElementException for empty resource" in {
    val reader = mock[HadoopParquetReader[RowParquetRecord]]
    reader.read() returns null

    a[NoSuchElementException] should be thrownBy newParquetIterable[TestRow](
      mockTestBuilder(reader),
      vcc,
      mock[Stats]
    ).iterator.next()
  }

  it should "try to read record only once in case of sequential calls for missing record" in {
    val reader = mock[HadoopParquetReader[RowParquetRecord]]
    reader.read() returns null

    val iterator = newParquetIterable[TestRow](mockTestBuilder(reader), vcc, mock[Stats]).iterator
    a[NoSuchElementException] should be thrownBy iterator.next()
    a[NoSuchElementException] should be thrownBy iterator.next()
    a[NoSuchElementException] should be thrownBy iterator.next()

    reader.read() wasCalled once
  }

  it should "read next records until there so no more with subsequent calls" in {
    val reader = mock[HadoopParquetReader[RowParquetRecord]]
    reader.read() returns testRecord(1) andThen testRecord(2) andThen testRecord(3) andThen null

    val iterator = newParquetIterable[TestRow](mockTestBuilder(reader), vcc, mock[Stats]).iterator
    iterator.next() should be(TestRow(1))
    iterator.next() should be(TestRow(2))
    iterator.next() should be(TestRow(3))
    a[NoSuchElementException] should be thrownBy iterator.next()
  }

  it should "not call 'read' if 'hasNext' already did it (and throw exception)" in {
    val reader = mock[HadoopParquetReader[RowParquetRecord]]
    reader.read() returns null

    val iterator = newParquetIterable[TestRow](mockTestBuilder(reader), vcc, mock[Stats]).iterator
    iterator.hasNext should be(false)
    a[NoSuchElementException] should be thrownBy iterator.next()

    reader.read() wasCalled once
  }

  it should "not call 'read' if 'hasNext' already did it (and return record)" in {
    val reader = mock[HadoopParquetReader[RowParquetRecord]]
    reader.read() returns testRecord(1)

    val iterator = newParquetIterable[TestRow](mockTestBuilder(reader), vcc, mock[Stats]).iterator
    iterator.hasNext should be(true)
    iterator.next() should be(TestRow(1))

    reader.read() wasCalled once
  }

  it should "not call 'read' if 'hasNext' already did it (return the only available record)" in {
    val reader = mock[HadoopParquetReader[RowParquetRecord]]
    reader.read() returns testRecord(1) andThen null

    val iterator = newParquetIterable[TestRow](mockTestBuilder(reader), vcc, mock[Stats]).iterator
    iterator.hasNext should be(true)
    iterator.next() should be(TestRow(1))
    iterator.hasNext should be(false)
    a[NoSuchElementException] should be thrownBy iterator.next()

    reader.read() wasCalled twice
  }

  it should "not call 'read' if 'hasNext' already did it (return two available records)" in {
    val reader = mock[HadoopParquetReader[RowParquetRecord]]
    reader.read() returns testRecord(1) andThen testRecord(2) andThen null

    val iterator = newParquetIterable[TestRow](mockTestBuilder(reader), vcc, mock[Stats]).iterator
    iterator.hasNext should be(true)
    iterator.next() should be(TestRow(1))
    iterator.hasNext should be(true)
    iterator.next() should be(TestRow(2))
    iterator.hasNext should be(false)
    a[NoSuchElementException] should be thrownBy iterator.next()

    reader.read() wasCalled 3.times
  }

  "close" should "close reader created by iterator" in {
    val reader = mock[HadoopParquetReader[RowParquetRecord]]

    val iterable = newParquetIterable[TestRow](mockTestBuilder(reader), vcc, mock[Stats])
    iterable.iterator
    iterable.close()

    reader.close() was called
  }

  it should "close all readers created by multiple iterators" in {
    val reader   = mock[HadoopParquetReader[RowParquetRecord]]
    val iterable = newParquetIterable[TestRow](mockTestBuilder(reader), vcc, mock[Stats])

    iterable.iterator
    iterable.iterator
    iterable.iterator

    iterable.close()

    reader.close() wasCalled 3.times
  }

  "size" should "use stats for returning record count" in {
    val stats = mock[Stats]
    newParquetIterable[TestRow](
      mock[HadoopParquetReader.Builder[RowParquetRecord]],
      vcc,
      stats
    ).size
    stats.recordCount was called
  }

  "min" should "use stats for returning record count" in {
    val stats = mock[Stats]
    newParquetIterable[TestRow](
      mock[HadoopParquetReader.Builder[RowParquetRecord]],
      vcc,
      stats
    ).min[Int](Col("int"))
    stats.min(Col("int"))(any[ValueCodec[Int]], any[Ordering[Int]]) was called
  }

  "max" should "use stats for returning record count" in {
    val stats = mock[Stats]
    newParquetIterable[TestRow](
      mock[HadoopParquetReader.Builder[RowParquetRecord]],
      vcc,
      stats
    ).max[Int](Col("int"))
    stats.max(Col("int"))(any[ValueCodec[Int]], any[Ordering[Int]]) was called
  }

}
