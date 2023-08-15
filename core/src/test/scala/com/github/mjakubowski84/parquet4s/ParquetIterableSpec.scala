package com.github.mjakubowski84.parquet4s

import ValueImplicits.*
import org.mockito.ArgumentMatchers.{any, eq as eqTo}
import org.mockito.Mockito.{times, verify, when}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import scala.reflect.{ClassTag, classTag}

object ParquetIterableSpec {

  case class TestRow(i: Int)

  val vcc: ValueCodecConfiguration = ValueCodecConfiguration.Default

  def mock[T: ClassTag]: T = org.mockito.Mockito.mock(classTag[T].runtimeClass).asInstanceOf[T]
}

class ParquetIterableSpec extends AnyFlatSpec with Matchers {

  import ParquetIterableSpec.*

  "iterator" should "build instance of iterator over row class containing record reader" in {
    val iteratorMock    = mock[ParquetIterator[RowParquetRecord]]
    val testRowIterator = Iterator.empty
    when(iteratorMock.map(any[RowParquetRecord => TestRow]())).thenReturn(testRowIterator)
    ParquetIterable
      .apply[TestRow](
        iteratorFactory         = () => iteratorMock,
        valueCodecConfiguration = vcc,
        stats                   = mock[Stats]
      )
      .iterator should be(testRowIterator)
  }

  it should "build a new iterator with new reader every time called" in {
    val iteratorMock        = mock[ParquetIterator[RowParquetRecord]]
    val iteratorFactoryMock = mock[() => ParquetIterator[RowParquetRecord]]
    when(iteratorFactoryMock.apply()).thenReturn(iteratorMock)
    val testRowIterator = Iterator.empty
    when(iteratorMock.map(any[RowParquetRecord => TestRow]())).thenReturn(testRowIterator)

    ParquetIterable.apply[TestRow](iteratorFactoryMock, vcc, mock[Stats]).iterator should be(testRowIterator)
    ParquetIterable.apply[TestRow](iteratorFactoryMock, vcc, mock[Stats]).iterator should be(testRowIterator)
    ParquetIterable.apply[TestRow](iteratorFactoryMock, vcc, mock[Stats]).iterator should be(testRowIterator)

    verify(iteratorFactoryMock, times(3)).apply()
  }

  "close" should "close reader created by iterator" in {
    val iteratorMock = mock[ParquetIterator[RowParquetRecord]]

    val iterable = ParquetIterable.apply[TestRow](() => iteratorMock, vcc, mock[Stats])
    iterable.iterator
    iterable.close()

    verify(iteratorMock).close()
  }

  it should "close all readers created by multiple iterators" in {
    val iteratorFactoryMock = mock[() => ParquetIterator[RowParquetRecord]]
    val iteratorMock1       = mock[ParquetIterator[RowParquetRecord]]
    val iteratorMock2       = mock[ParquetIterator[RowParquetRecord]]
    val iteratorMock3       = mock[ParquetIterator[RowParquetRecord]]
    when(iteratorFactoryMock.apply())
      .thenReturn(iteratorMock1)
      .thenReturn(iteratorMock2)
      .thenReturn(iteratorMock3)
    val iterable = ParquetIterable.apply[TestRow](iteratorFactoryMock, vcc, mock[Stats])

    iterable.iterator
    iterable.iterator
    iterable.iterator

    iterable.close()

    verify(iteratorMock1).close()
    verify(iteratorMock2).close()
    verify(iteratorMock3).close()
  }

  "size" should "use stats for returning record count" in {
    val stats = mock[Stats]
    ParquetIterable
      .apply[TestRow](
        iteratorFactory         = () => mock[ParquetIterator[RowParquetRecord]],
        valueCodecConfiguration = vcc,
        stats                   = stats
      )
      .size

    verify(stats).recordCount
  }

  "min" should "use stats for returning record count" in {
    val stats = mock[Stats]
    ParquetIterable
      .apply[TestRow](
        iteratorFactory         = () => mock[ParquetIterator[RowParquetRecord]],
        valueCodecConfiguration = vcc,
        stats                   = stats
      )
      .min[Int](Col("i"))

    verify(stats).min(eqTo(Col("i")))(any[ValueDecoder[Int]], any[Ordering[Int]])
  }

  "max" should "use stats for returning record count" in {
    val stats = mock[Stats]
    ParquetIterable
      .apply[TestRow](
        iteratorFactory         = () => mock[ParquetIterator[RowParquetRecord]],
        valueCodecConfiguration = vcc,
        stats                   = stats
      )
      .max[Int](Col("i"))

    verify(stats).max(eqTo(Col("i")))(any[ValueDecoder[Int]], any[Ordering[Int]])
  }

  "concat" should "append one dataset to another" in {
    val a = Seq(
      RowParquetRecord("i" -> 1.value),
      RowParquetRecord("i" -> 2.value),
      RowParquetRecord("i" -> 3.value)
    )
    val b = Seq(
      RowParquetRecord("i" -> 4.value),
      RowParquetRecord("i" -> 5.value),
      RowParquetRecord("i" -> 6.value)
    )
    ParquetIterable[RowParquetRecord](() => ParquetIterator.from(a*), vcc, mock[Stats])
      .concat(ParquetIterable[RowParquetRecord](() => ParquetIterator.from(b*), vcc, mock[Stats]))
      .toSeq should be(a ++ b)
  }

  it should "combine two empty datasets" in {
    ParquetIterable[RowParquetRecord](() => ParquetIterator.from(), vcc, mock[Stats])
      .concat(ParquetIterable[RowParquetRecord](() => ParquetIterator.from(), vcc, mock[Stats]))
      .toSeq should be(empty)
  }

  it should "append non-empty dataset to empty one" in {
    val b = Seq(
      RowParquetRecord("i" -> 4.value),
      RowParquetRecord("i" -> 5.value),
      RowParquetRecord("i" -> 6.value)
    )
    ParquetIterable[RowParquetRecord](() => ParquetIterator.from(), vcc, mock[Stats])
      .concat(ParquetIterable[RowParquetRecord](() => ParquetIterator.from(b*), vcc, mock[Stats]))
      .toSeq should be(b)
  }

  it should "append empty dataset to non-empty one" in {
    val a = Seq(
      RowParquetRecord("i" -> 1.value),
      RowParquetRecord("i" -> 2.value),
      RowParquetRecord("i" -> 3.value)
    )
    ParquetIterable[RowParquetRecord](() => ParquetIterator.from(a*), vcc, mock[Stats])
      .concat(ParquetIterable[RowParquetRecord](() => ParquetIterator.from(), vcc, mock[Stats]))
      .toSeq should be(a)
  }

  "as" should "map raw content of iterable to types" in {
    val a = Seq(
      RowParquetRecord("i" -> 1.value),
      RowParquetRecord("i" -> 2.value),
      RowParquetRecord("i" -> 3.value)
    )
    ParquetIterable[RowParquetRecord](() => ParquetIterator.from(a*), vcc, mock[Stats])
      .as[TestRow]
      .toSeq should be(Seq(TestRow(1), TestRow(2), TestRow(3)))
  }

  it should "map empty iterable" in {
    ParquetIterable[RowParquetRecord](() => ParquetIterator.from(), vcc, mock[Stats])
      .as[TestRow]
      .toSeq should be(empty)
  }

  it should "map concatenated records" in {
    val a = Seq(
      RowParquetRecord("i" -> 1.value),
      RowParquetRecord("i" -> 2.value),
      RowParquetRecord("i" -> 3.value)
    )
    val b = Seq(
      RowParquetRecord("i" -> 4.value),
      RowParquetRecord("i" -> 5.value),
      RowParquetRecord("i" -> 6.value)
    )
    ParquetIterable[RowParquetRecord](() => ParquetIterator.from(a*), vcc, mock[Stats])
      .concat(ParquetIterable[RowParquetRecord](() => ParquetIterator.from(b*), vcc, mock[Stats]))
      .as[TestRow]
      .toSeq
      .map(_.i) should be(Seq(1, 2, 3, 4, 5, 6))
  }

}
