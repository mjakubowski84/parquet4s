package com.github.mjakubowski84.parquet4s

import com.github.mjakubowski84.parquet4s
import com.github.mjakubowski84.parquet4s.ValueImplicits.*
import org.apache.parquet.hadoop.ParquetReader as HadoopParquetReader
import org.mockito.Mockito.{times, verify, when}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import scala.reflect.{ClassTag, classTag}

class ParquetIteratorSpec extends AnyFlatSpec with Matchers {

  private def testRecord(int: Int): RowParquetRecord = RowParquetRecord("int" -> int.value)

  private def mock[T: ClassTag]: T = org.mockito.Mockito.mock(classTag[T].runtimeClass).asInstanceOf[T]

  private def mockTestBuilder(reader: HadoopParquetReader[RowParquetRecord]) = {
    val builder = mock[parquet4s.ParquetIterator.HadoopBuilder]
    when(builder.build()).thenReturn(reader)
    builder
  }

  "hasNext" should "return false for empty resource" in {
    val reader = mock[HadoopParquetReader[RowParquetRecord]]
    when(reader.read()).thenReturn(null)

    new ParquetIterator(mockTestBuilder(reader)).hasNext should be(false)

    verify(reader).read()
  }

  it should "return true for single-record resource" in {
    val reader = mock[HadoopParquetReader[RowParquetRecord]]
    when(reader.read()).thenReturn(testRecord(1))

    new ParquetIterator(mockTestBuilder(reader)).hasNext should be(true)

    verify(reader).read()
  }

  it should "call 'read' when it is called itself multiple times in sequence (and return false)" in {
    val reader = mock[HadoopParquetReader[RowParquetRecord]]
    when(reader.read()).thenReturn(null)

    val iterator = new ParquetIterator(mockTestBuilder(reader))
    iterator.hasNext should be(false)
    iterator.hasNext should be(false)
    iterator.hasNext should be(false)

    verify(reader).read()
  }

  it should "call 'read' when it is called itself multiple times in sequence (and return true)" in {
    val reader = mock[HadoopParquetReader[RowParquetRecord]]
    when(reader.read()).thenReturn(testRecord(1))

    val iterator = new ParquetIterator(mockTestBuilder(reader))
    iterator.hasNext should be(true)
    iterator.hasNext should be(true)
    iterator.hasNext should be(true)

    verify(reader).read()
  }

  "next" should "return row for single-record resource" in {
    val reader = mock[HadoopParquetReader[RowParquetRecord]]
    when(reader.read()).thenReturn(testRecord(1))

    new ParquetIterator(mockTestBuilder(reader)).next() should be(testRecord(1))
  }

  it should "throw NoSuchElementException for empty resource" in {
    val reader = mock[HadoopParquetReader[RowParquetRecord]]
    when(reader.read()).thenReturn(null)

    a[NoSuchElementException] should be thrownBy new ParquetIterator(mockTestBuilder(reader)).next()
  }

  it should "try to read record only once in case of sequential calls for missing record" in {
    val reader = mock[HadoopParquetReader[RowParquetRecord]]
    when(reader.read()).thenReturn(null)

    val iterator = new ParquetIterator(mockTestBuilder(reader))
    a[NoSuchElementException] should be thrownBy iterator.next()
    a[NoSuchElementException] should be thrownBy iterator.next()
    a[NoSuchElementException] should be thrownBy iterator.next()

    verify(reader).read()
  }

  it should "read next records until there so no more with subsequent calls" in {
    val reader = mock[HadoopParquetReader[RowParquetRecord]]
    when(reader.read())
      .thenReturn(testRecord(1))
      .thenReturn(testRecord(2))
      .thenReturn(testRecord(3))
      .thenReturn(null)

    val iterator = new ParquetIterator(mockTestBuilder(reader))
    iterator.next() should be(testRecord(1))
    iterator.next() should be(testRecord(2))
    iterator.next() should be(testRecord(3))
    a[NoSuchElementException] should be thrownBy iterator.next()
  }

  it should "not call 'read' if 'hasNext' already did it (and throw exception)" in {
    val reader = mock[HadoopParquetReader[RowParquetRecord]]
    when(reader.read()).thenReturn(null)

    val iterator = new ParquetIterator(mockTestBuilder(reader))
    iterator.hasNext should be(false)
    a[NoSuchElementException] should be thrownBy iterator.next()

    verify(reader).read()
  }

  it should "not call 'read' if 'hasNext' already did it (and return record)" in {
    val reader = mock[HadoopParquetReader[RowParquetRecord]]
    when(reader.read()).thenReturn(testRecord(1))

    val iterator = new ParquetIterator(mockTestBuilder(reader))
    iterator.hasNext should be(true)
    iterator.next() should be(testRecord(1))

    verify(reader).read()
  }

  it should "not call 'read' if 'hasNext' already did it (return the only available record)" in {
    val reader = mock[HadoopParquetReader[RowParquetRecord]]
    when(reader.read())
      .thenReturn(testRecord(1))
      .thenReturn(null)

    val iterator = new ParquetIterator(mockTestBuilder(reader))
    iterator.hasNext should be(true)
    iterator.next() should be(testRecord(1))
    iterator.hasNext should be(false)
    a[NoSuchElementException] should be thrownBy iterator.next()

    verify(reader, times(2)).read()
  }

  it should "not call 'read' if 'hasNext' already did it (return two available records)" in {
    val reader = mock[HadoopParquetReader[RowParquetRecord]]
    when(reader.read())
      .thenReturn(testRecord(1))
      .thenReturn(testRecord(2))
      .thenReturn(null)

    val iterator = new ParquetIterator(mockTestBuilder(reader))
    iterator.hasNext should be(true)
    iterator.next() should be(testRecord(1))
    iterator.hasNext should be(true)
    iterator.next() should be(testRecord(2))
    iterator.hasNext should be(false)
    a[NoSuchElementException] should be thrownBy iterator.next()

    verify(reader, times(3)).read()
  }

  "close" should "close the wrapped reader" in {
    val reader = mock[HadoopParquetReader[RowParquetRecord]]
    when(reader.read()).thenReturn(null)

    new ParquetIterator(mockTestBuilder(reader)).close()

    verify(reader).close()
  }

}
