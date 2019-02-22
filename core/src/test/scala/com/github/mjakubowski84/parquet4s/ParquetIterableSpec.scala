package com.github.mjakubowski84.parquet4s

import java.nio.file.Paths

import com.google.common.io.Files
import org.apache.hadoop.fs.Path
import org.apache.parquet.hadoop.{ParquetReader => HadoopParquetReader}
import org.scalamock.scalatest.MockFactory
import org.scalatest.{FlatSpec, Matchers}
import ValueImplicits._

object ParquetIterableSpec {

  case class TestRow(int: Int)

  def testRecord(int: Int): RowParquetRecord = RowParquetRecord("int" -> int)

  private lazy val path = new Path(Paths.get(Files.createTempDir().getAbsolutePath).toString)
  class TestReader extends HadoopParquetReader[RowParquetRecord](path, null)
  class TestBuilder extends ParquetReader.Builder(path)

  val options: ParquetReader.Options = ParquetReader.Options()
}

class ParquetIterableSpec extends FlatSpec with Matchers with MockFactory {

  import ParquetIterableSpec._

  "iterator" should "build instance of iterator over row class containing record reader" in {
    val builder = mock[TestBuilder]
    (builder.build _).expects().returns(mock[TestReader])

    ParquetReader.newParquetIterable[TestRow](builder, options).iterator should be(an[Iterator[_]])
  }

  it should "build a new iterator with new reader every time called" in {
    val builder = mock[TestBuilder]
    (builder.build _).expects().returns(mock[TestReader]).repeated(3)

    ParquetReader.newParquetIterable[TestRow](builder, options).iterator should be(an[Iterator[_]])
    ParquetReader.newParquetIterable[TestRow](builder, options).iterator should be(an[Iterator[_]])
    ParquetReader.newParquetIterable[TestRow](builder, options).iterator should be(an[Iterator[_]])
  }

  "hasNext" should "return false for empty resource" in {
    val reader = mock[TestReader]
    (reader.read _).expects().returns(null).once()

    val builder = mock[TestBuilder]
    (builder.build _).expects().returns(reader)

    ParquetReader.newParquetIterable[TestRow](builder, options).iterator.hasNext should be(false)
  }

  it should "return true for single-record resource" in {
    val reader = mock[TestReader]
    (reader.read _).expects().returns(testRecord(1)).once()

    val builder = mock[TestBuilder]
    (builder.build _).expects().returns(reader)

    ParquetReader.newParquetIterable[TestRow](builder, options).iterator.hasNext should be(true)
  }

  it should "call 'read' when it is called itself multiple times in sequence (and return false)" in {
    val reader = mock[TestReader]
    (reader.read _).expects().returns(null).once()

    val builder = mock[TestBuilder]
    (builder.build _).expects().returns(reader)

    val iterator = ParquetReader.newParquetIterable[TestRow](builder, options).iterator
    iterator.hasNext should be(false)
    iterator.hasNext should be(false)
    iterator.hasNext should be(false)
  }

  it should "call 'read' when it is called itself multiple times in sequence (and return true)" in {
    val reader = mock[TestReader]
    (reader.read _).expects().returns(testRecord(1)).once()

    val builder = mock[TestBuilder]
    (builder.build _).expects().returns(reader)

    val iterator = ParquetReader.newParquetIterable[TestRow](builder, options).iterator
    iterator.hasNext should be(true)
    iterator.hasNext should be(true)
    iterator.hasNext should be(true)
  }

  "next" should "throw NoSuchElementException for empty resource" in {
    val reader = mock[TestReader]
    (reader.read _).expects().returns(testRecord(1)).once()

    val builder = mock[TestBuilder]
    (builder.build _).expects().returns(reader)

    ParquetReader.newParquetIterable[TestRow](builder, options).iterator.next should be(TestRow(1))
  }

  it should "return row for single-record resource" in {
    val reader = mock[TestReader]
    (reader.read _).expects().returns(null).once()

    val builder = mock[TestBuilder]
    (builder.build _).expects().returns(reader)

    a[NoSuchElementException] should be thrownBy ParquetReader.newParquetIterable[TestRow](builder, options).iterator.next
  }

  it should "try to read record only once in case of sequential calls for missing record" in {
    val reader = mock[TestReader]
    (reader.read _).expects().returns(null).once()

    val builder = mock[TestBuilder]
    (builder.build _).expects().returns(reader)

    val iterator = ParquetReader.newParquetIterable[TestRow](builder, options).iterator
    a[NoSuchElementException] should be thrownBy iterator.next
    a[NoSuchElementException] should be thrownBy iterator.next
    a[NoSuchElementException] should be thrownBy iterator.next
  }

  it should "read next records until there so no more with subsequent calls" in {
    val reader = mock[TestReader]
    (reader.read _).expects().returns(testRecord(1))
    (reader.read _).expects().returns(testRecord(2))
    (reader.read _).expects().returns(testRecord(3))
    (reader.read _).expects().returns(null)

    val builder = mock[TestBuilder]
    (builder.build _).expects().returns(reader)

    val iterator = ParquetReader.newParquetIterable[TestRow](builder, options).iterator
    iterator.next should be(TestRow(1))
    iterator.next should be(TestRow(2))
    iterator.next should be(TestRow(3))
    a[NoSuchElementException] should be thrownBy iterator.next
  }

  it should "not call 'read' if 'hasNext' already did it (and throw exception)" in {
    val reader = mock[TestReader]
    (reader.read _).expects().returns(null).once()

    val builder = mock[TestBuilder]
    (builder.build _).expects().returns(reader)

    val iterator = ParquetReader.newParquetIterable[TestRow](builder, options).iterator
    iterator.hasNext should be(false)
    a[NoSuchElementException] should be thrownBy iterator.next
  }

  it should "not call 'read' if 'hasNext' already did it (and return record)" in {
    val reader = mock[TestReader]
    (reader.read _).expects().returns(testRecord(1)).once()

    val builder = mock[TestBuilder]
    (builder.build _).expects().returns(reader)

    val iterator = ParquetReader.newParquetIterable[TestRow](builder, options).iterator
    iterator.hasNext should be(true)
    iterator.next should be(TestRow(1))
  }

  it should "not call 'read' if 'hasNext' already did it (return the only available record)" in {
    val reader = mock[TestReader]
    (reader.read _).expects().returns(testRecord(1)).once()
    (reader.read _).expects().returns(null).once()

    val builder = mock[TestBuilder]
    (builder.build _).expects().returns(reader)

    val iterator = ParquetReader.newParquetIterable[TestRow](builder, options).iterator
    iterator.hasNext should be(true)
    iterator.next should be(TestRow(1))
    iterator.hasNext should be(false)
    a[NoSuchElementException] should be thrownBy iterator.next
  }

  it should "not call 'read' if 'hasNext' already did it (return two available records)" in {
    val reader = mock[TestReader]
    (reader.read _).expects().returns(testRecord(1)).once()
    (reader.read _).expects().returns(testRecord(2)).once()
    (reader.read _).expects().returns(null).once()

    val builder = mock[TestBuilder]
    (builder.build _).expects().returns(reader)

    val iterator = ParquetReader.newParquetIterable[TestRow](builder, options).iterator
    iterator.hasNext should be(true)
    iterator.next should be(TestRow(1))
    iterator.hasNext should be(true)
    iterator.next should be(TestRow(2))
    iterator.hasNext should be(false)
    a[NoSuchElementException] should be thrownBy iterator.next
  }

  "close" should "close reader created by iterator" in {
    val reader = mock[TestReader]
    (reader.close _).expects().returns(()).once()

    val builder = mock[TestBuilder]
    (builder.build _).expects().returns(reader)

    val iterable = ParquetReader.newParquetIterable[TestRow](builder, options)
    iterable.iterator
    iterable.close()
  }

  it should "close all readers created by multiple iterators" in {
    val reader = mock[TestReader]
    (reader.close _).expects().returns(()).repeated(3)

    val builder = mock[TestBuilder]
    (builder.build _).expects().returns(reader).repeated(3)

    val iterable = ParquetReader.newParquetIterable[TestRow](builder, options)

    iterable.iterator
    iterable.iterator
    iterable.iterator

    iterable.close()
  }

}
