package com.github.mjakubowski84.parquet4s

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import ValueImplicits.*

class JoinSpec extends AnyFlatSpec with Matchers {

  "leftJoin" should "perform left join on two datasets" in {
    val left = new InMemoryParquetIterable[RowParquetRecord](
      data = Seq(
        RowParquetRecord("left" -> "A".value, "onLeft" -> 1.value),
        RowParquetRecord("left" -> "B".value, "onLeft" -> 2.value),
        RowParquetRecord("left" -> "C".value, "onLeft" -> 3.value)
      )
    )
    val right = new InMemoryParquetIterable[RowParquetRecord](
      data = Seq(
        RowParquetRecord("right" -> "X".value, "onRight" -> 11.value),
        RowParquetRecord("right" -> "Y".value, "onRight" -> 2.value),
        RowParquetRecord("right" -> "Z".value, "onRight" -> 2.value)
      )
    )

    left.leftJoin(right, onLeft = Col("onLeft"), onRight = Col("onRight")).toSeq should be(
      Seq(
        RowParquetRecord("left" -> "A".value, "onLeft" -> 1.value, "right" -> NullValue, "onRight" -> NullValue),
        RowParquetRecord("left" -> "B".value, "onLeft" -> 2.value, "right" -> "Y".value, "onRight" -> 2.value),
        RowParquetRecord("left" -> "B".value, "onLeft" -> 2.value, "right" -> "Z".value, "onRight" -> 2.value),
        RowParquetRecord("left" -> "C".value, "onLeft" -> 3.value, "right" -> NullValue, "onRight" -> NullValue)
      )
    )
  }

  it should "perform left join on empty datasets" in {
    val nonEmptyLeft = new InMemoryParquetIterable[RowParquetRecord](
      data = Seq(RowParquetRecord("left" -> "A".value, "onLeft" -> 1.value))
    )
    val nonEmptyRight = new InMemoryParquetIterable[RowParquetRecord](
      data = Seq(RowParquetRecord("right" -> "X".value, "onRight" -> 2.value))
    )
    val emptyDataset = new InMemoryParquetIterable[RowParquetRecord](data = Iterable.empty)

    nonEmptyLeft.leftJoin(nonEmptyRight, onLeft = Col("onLeft"), onRight = Col("onRight")).toSeq should be(
      Seq(RowParquetRecord("left" -> "A".value, "onLeft" -> 1.value, "right" -> NullValue, "onRight" -> NullValue))
    )
    nonEmptyLeft.leftJoin(emptyDataset, onLeft = Col("onLeft"), onRight = Col("onRight")).toSeq should be(
      Seq(RowParquetRecord("left" -> "A".value, "onLeft" -> 1.value))
    )
    emptyDataset.leftJoin(nonEmptyRight, onLeft = Col("onLeft"), onRight = Col("onRight")).toSeq should be(empty)
    emptyDataset.leftJoin(emptyDataset, onLeft = Col("onLeft"), onRight = Col("onRight")).toSeq should be(empty)
  }

  "rightJoin" should "perform right join on two datasets" in {
    val left = new InMemoryParquetIterable[RowParquetRecord](
      data = Seq(
        RowParquetRecord("left" -> "A".value, "onLeft" -> 11.value),
        RowParquetRecord("left" -> "B".value, "onLeft" -> 2.value),
        RowParquetRecord("left" -> "C".value, "onLeft" -> 2.value)
      )
    )
    val right = new InMemoryParquetIterable[RowParquetRecord](
      data = Seq(
        RowParquetRecord("right" -> "X".value, "onRight" -> 1.value),
        RowParquetRecord("right" -> "Y".value, "onRight" -> 2.value),
        RowParquetRecord("right" -> "Z".value, "onRight" -> 3.value)
      )
    )

    left.rightJoin(right, onLeft = Col("onLeft"), onRight = Col("onRight")).toSet should be(
      Set(
        RowParquetRecord("left" -> "B".value, "onLeft" -> 2.value, "right" -> "Y".value, "onRight" -> 2.value),
        RowParquetRecord("left" -> "C".value, "onLeft" -> 2.value, "right" -> "Y".value, "onRight" -> 2.value),
        RowParquetRecord("left" -> NullValue, "onLeft" -> NullValue, "right" -> "X".value, "onRight" -> 1.value),
        RowParquetRecord("left" -> NullValue, "onLeft" -> NullValue, "right" -> "Z".value, "onRight" -> 3.value)
      )
    )
  }

  it should "perform right join on empty datasets" in {
    val nonEmptyLeft = new InMemoryParquetIterable[RowParquetRecord](
      data = Seq(RowParquetRecord("left" -> "A".value, "onLeft" -> 1.value))
    )
    val nonEmptyRight = new InMemoryParquetIterable[RowParquetRecord](
      data = Seq(RowParquetRecord("right" -> "X".value, "onRight" -> 2.value))
    )
    val emptyDataset = new InMemoryParquetIterable[RowParquetRecord](data = Iterable.empty)

    nonEmptyLeft.rightJoin(nonEmptyRight, onLeft = Col("onLeft"), onRight = Col("onRight")).toSeq should be(
      Seq(RowParquetRecord("left" -> NullValue, "onLeft" -> NullValue, "right" -> "X".value, "onRight" -> 2.value))
    )
    nonEmptyLeft.rightJoin(emptyDataset, onLeft = Col("onLeft"), onRight = Col("onRight")).toSeq should be(empty)
    emptyDataset.rightJoin(nonEmptyRight, onLeft = Col("onLeft"), onRight = Col("onRight")).toSeq should be(
      Seq(RowParquetRecord("right" -> "X".value, "onRight" -> 2.value))
    )
    emptyDataset.rightJoin(emptyDataset, onLeft = Col("onLeft"), onRight = Col("onRight")).toSeq should be(empty)
  }

  "innerJoin" should "perform inner join on two datasets" in {
    val left = new InMemoryParquetIterable[RowParquetRecord](
      data = Seq(
        RowParquetRecord("left" -> "A".value, "onLeft" -> 1.value),
        RowParquetRecord("left" -> "B".value, "onLeft" -> 2.value),
        RowParquetRecord("left" -> "C".value, "onLeft" -> 3.value)
      )
    )
    val right = new InMemoryParquetIterable[RowParquetRecord](
      data = Seq(
        RowParquetRecord("right" -> "X".value, "onRight" -> 11.value),
        RowParquetRecord("right" -> "Y".value, "onRight" -> 2.value),
        RowParquetRecord("right" -> "Z".value, "onRight" -> 2.value)
      )
    )

    left.innerJoin(right, onLeft = Col("onLeft"), onRight = Col("onRight")).toSeq should be(
      Seq(
        RowParquetRecord("left" -> "B".value, "onLeft" -> 2.value, "right" -> "Y".value, "onRight" -> 2.value),
        RowParquetRecord("left" -> "B".value, "onLeft" -> 2.value, "right" -> "Z".value, "onRight" -> 2.value)
      )
    )
  }

  it should "perform inner join on empty datasets" in {
    val nonEmptyLeft = new InMemoryParquetIterable[RowParquetRecord](
      data = Seq(RowParquetRecord("left" -> "A".value, "onLeft" -> 1.value))
    )
    val nonEmptyRight = new InMemoryParquetIterable[RowParquetRecord](
      data = Seq(RowParquetRecord("right" -> "X".value, "onRight" -> 2.value))
    )
    val emptyDataset = new InMemoryParquetIterable[RowParquetRecord](data = Iterable.empty)

    nonEmptyLeft.innerJoin(nonEmptyRight, onLeft = Col("onLeft"), onRight = Col("onRight")).toSeq should be(empty)
    nonEmptyLeft.innerJoin(emptyDataset, onLeft = Col("onLeft"), onRight = Col("onRight")).toSeq should be(empty)
    emptyDataset.innerJoin(nonEmptyRight, onLeft = Col("onLeft"), onRight = Col("onRight")).toSeq should be(empty)
    emptyDataset.innerJoin(emptyDataset, onLeft = Col("onLeft"), onRight = Col("onRight")).toSeq should be(empty)
  }

  "fullJoin" should "perform full join on two datasets" in {
    val left = new InMemoryParquetIterable[RowParquetRecord](
      data = Seq(
        RowParquetRecord("left" -> "A".value, "onLeft" -> 11.value),
        RowParquetRecord("left" -> "B".value, "onLeft" -> 2.value),
        RowParquetRecord("left" -> "C".value, "onLeft" -> 2.value)
      )
    )
    val right = new InMemoryParquetIterable[RowParquetRecord](
      data = Seq(
        RowParquetRecord("right" -> "X".value, "onRight" -> 1.value),
        RowParquetRecord("right" -> "Y".value, "onRight" -> 2.value),
        RowParquetRecord("right" -> "Z".value, "onRight" -> 3.value)
      )
    )

    left.fullJoin(right, onLeft = Col("onLeft"), onRight = Col("onRight")).toSet should be(
      Set(
        RowParquetRecord("left" -> "A".value, "onLeft" -> 11.value, "right" -> NullValue, "onRight" -> NullValue),
        RowParquetRecord("left" -> "B".value, "onLeft" -> 2.value, "right" -> "Y".value, "onRight" -> 2.value),
        RowParquetRecord("left" -> "C".value, "onLeft" -> 2.value, "right" -> "Y".value, "onRight" -> 2.value),
        RowParquetRecord("left" -> NullValue, "onLeft" -> NullValue, "right" -> "X".value, "onRight" -> 1.value),
        RowParquetRecord("left" -> NullValue, "onLeft" -> NullValue, "right" -> "Z".value, "onRight" -> 3.value)
      )
    )
  }

  it should "perform full join on empty datasets" in {
    val nonEmptyLeft = new InMemoryParquetIterable[RowParquetRecord](
      data = Seq(RowParquetRecord("left" -> "A".value, "onLeft" -> 1.value))
    )
    val nonEmptyRight = new InMemoryParquetIterable[RowParquetRecord](
      data = Seq(RowParquetRecord("right" -> "X".value, "onRight" -> 2.value))
    )
    val emptyDataset = new InMemoryParquetIterable[RowParquetRecord](data = Iterable.empty)

    nonEmptyLeft.fullJoin(nonEmptyRight, onLeft = Col("onLeft"), onRight = Col("onRight")).toSeq should be(
      Seq(
        RowParquetRecord("left" -> "A".value, "onLeft" -> 1.value, "right" -> NullValue, "onRight" -> NullValue),
        RowParquetRecord("left" -> NullValue, "onLeft" -> NullValue, "right" -> "X".value, "onRight" -> 2.value)
      )
    )
    nonEmptyLeft.fullJoin(emptyDataset, onLeft = Col("onLeft"), onRight = Col("onRight")).toSeq should be(
      Seq(RowParquetRecord("left" -> "A".value, "onLeft" -> 1.value))
    )
    emptyDataset.fullJoin(nonEmptyRight, onLeft = Col("onLeft"), onRight = Col("onRight")).toSeq should be(
      Seq(RowParquetRecord("right" -> "X".value, "onRight" -> 2.value))
    )
    emptyDataset.fullJoin(emptyDataset, onLeft = Col("onLeft"), onRight = Col("onRight")).toSeq should be(empty)
  }

}
