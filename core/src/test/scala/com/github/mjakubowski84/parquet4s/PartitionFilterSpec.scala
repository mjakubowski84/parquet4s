package com.github.mjakubowski84.parquet4s

import com.github.mjakubowski84.parquet4s.FilterRewriter.{IsFalse, IsTrue}
import com.github.mjakubowski84.parquet4s.PartitionFilterRewriter.AssumeTrue
import com.github.mjakubowski84.parquet4s.PartitionFilterSpec.IsUppercase
import com.github.mjakubowski84.parquet4s.PartitionedPath.Partition
import org.apache.commons.lang3.StringUtils
import org.apache.hadoop.conf.Configuration
import org.apache.parquet.filter2.predicate.FilterApi.{
  and as AND,
  eq as EQ,
  gt as GT,
  gtEq as GTE,
  lt as LT,
  ltEq as LTE,
  not as NOT,
  notEq as NEQ,
  or as OR,
  userDefined as UDP,
  *
}
import org.apache.parquet.filter2.predicate.{FilterPredicate, Operators, Statistics, UserDefinedPredicate}
import org.apache.parquet.io.api.Binary
import org.scalatest.{EitherValues, Inside}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

object PartitionFilterSpec {

  class IsUppercase extends UserDefinedPredicate[Binary] with Serializable {
    override def keep(value: Binary): Boolean                     = StringUtils.isAllUpperCase(value.toStringUsingUTF8)
    override def canDrop(statistics: Statistics[Binary]): Boolean = false
    override def inverseCanDrop(statistics: Statistics[Binary]): Boolean = false
    override val toString: String                                        = "is_uppercase"
  }

}

class PartitionFilterSpec extends AnyFlatSpec with Matchers with Inside with EitherValues {

  val (i, j, k, l, other) = (Col("i"), Col("j"), Col("k"), Col("l"), Col("other"))
  val (colI, colJ, colK, colL) =
    (binaryColumn(i.toString), binaryColumn(j.toString), binaryColumn(k.toString), binaryColumn(l.toString))

  val eqi: Operators.Eq[Binary]                        = EQ(colI, Binary.fromString("I"))
  val neqj: Operators.NotEq[Binary]                    = NEQ(colJ, Binary.fromString("J"))
  val gtk: Operators.Gt[Binary]                        = GT(colK, Binary.fromString("a"))
  val ltl: Operators.Lt[Binary]                        = LT(colL, Binary.fromString("z"))
  val gtek: Operators.GtEq[Binary]                     = GTE(colK, Binary.fromString("a"))
  val ltel: Operators.LtEq[Binary]                     = LTE(colL, Binary.fromString("z"))
  val udpi: Operators.UserDefined[Binary, IsUppercase] = UDP(colI, new IsUppercase)

  val case1: FilterPredicate = OR(AND(eqi, neqj), AND(gtk, ltl))
  val case2: FilterPredicate = AND(OR(udpi, neqj), OR(gtek, ltel))
  val case3: FilterPredicate = NOT(AND(udpi, neqj))
  val case4: FilterPredicate = NOT(OR(eqi, neqj))

  val vcc: ValueCodecConfiguration = ValueCodecConfiguration.Default
  val configuration                = new Configuration()

  def partitionedPath(partitions: Partition*): PartitionedPath =
    PartitionedPath(
      path               = Path("/"),
      configuration      = configuration,
      partitions         = partitions.toList,
      filterPredicateOpt = None
    )

  def partitionedDirectory(partitionedPaths: PartitionedPath*): PartitionedDirectory =
    PartitionedDirectory(partitionedPaths).value

  "PartitionedDirectory" should "accept empty input" in {
    val dir = partitionedDirectory()
    dir.schema should be(empty)
    dir.paths should be(empty)
  }

  it should "accept paths without partitions" in {
    val dir = partitionedDirectory(partitionedPath(), partitionedPath(), partitionedPath())
    dir.schema should be(empty)
    dir.paths should have size 3
  }

  it should "accept single partitioned path" in {
    val dir = partitionedDirectory(partitionedPath(i -> "4", j -> "2"))
    dir.schema should be(List(i, j))
    dir.paths should have size 1
  }

  it should "rise exception if number of partitions is inconsistent among paths" in {
    PartitionedDirectory(
      Seq(
        partitionedPath(i -> "1", j -> "A"),
        partitionedPath(i -> "2"),
        partitionedPath(i -> "3", j -> "C")
      )
    ).left.value should be(an[IllegalArgumentException])
  }

  it should "rise exception if order of partitions is inconsistent among paths" in {
    PartitionedDirectory(
      Seq(
        partitionedPath(i -> "1", j -> "A"),
        partitionedPath(j -> "B", i -> "2"),
        partitionedPath(i -> "3", j -> "C")
      )
    ).left.value should be(an[IllegalArgumentException])
  }

  "PartitionFilter visitor" should "handle case when the predicate does not match any column" in {
    eqi.accept(new PartitionFilter(PartitionView())) should be(false)
    eqi.accept(new PartitionFilter(PartitionView(other -> "other"))) should be(false)
  }

  it should "handle case when predicate matches column but is of non-binary type" in {
    val intPredicate = EQ(intColumn("i"), java.lang.Integer.valueOf(100))
    an[IllegalArgumentException] should be thrownBy intPredicate.accept(
      new PartitionFilter(PartitionView(i -> "A"))
    )
  }

  it should "process EQ predicate" in {
    eqi.accept(new PartitionFilter(PartitionView(i -> "A"))) should be(false)
    eqi.accept(new PartitionFilter(PartitionView(i -> "I"))) should be(true)
  }

  it should "process NEQ predicate" in {
    neqj.accept(new PartitionFilter(PartitionView(j -> "A"))) should be(true)
    neqj.accept(new PartitionFilter(PartitionView(j -> "J"))) should be(false)
  }

  it should "process GT predicate" in {
    gtk.accept(new PartitionFilter(PartitionView(k -> "A"))) should be(false)
    gtk.accept(new PartitionFilter(PartitionView(k -> "a"))) should be(false)
    gtk.accept(new PartitionFilter(PartitionView(k -> "b"))) should be(true)
  }

  it should "process GTE predicate" in {
    gtek.accept(new PartitionFilter(PartitionView(k -> "A"))) should be(false)
    gtek.accept(new PartitionFilter(PartitionView(k -> "a"))) should be(true)
    gtek.accept(new PartitionFilter(PartitionView(k -> "b"))) should be(true)
  }

  it should "process LT predicate" in {
    ltl.accept(new PartitionFilter(PartitionView(l -> "ż"))) should be(false)
    ltl.accept(new PartitionFilter(PartitionView(l -> "z"))) should be(false)
    ltl.accept(new PartitionFilter(PartitionView(l -> "y"))) should be(true)
  }

  it should "process LTE predicate" in {
    ltel.accept(new PartitionFilter(PartitionView(l -> "ż"))) should be(false)
    ltel.accept(new PartitionFilter(PartitionView(l -> "z"))) should be(true)
    ltel.accept(new PartitionFilter(PartitionView(l -> "y"))) should be(true)
  }

  it should "process UDP" in {
    udpi.accept(new PartitionFilter(PartitionView(i -> "ABC"))) should be(true)
    udpi.accept(new PartitionFilter(PartitionView(i -> "abc"))) should be(false)
  }

  it should "process AND predicate" in {
    val predicate = AND(eqi, neqj)
    predicate.accept(new PartitionFilter(PartitionView(i -> "I", j -> "A"))) should be(true)
    predicate.accept(new PartitionFilter(PartitionView(i -> "I", j -> "J"))) should be(false)
    predicate.accept(new PartitionFilter(PartitionView(i -> "X", j -> "A"))) should be(false)
    predicate.accept(new PartitionFilter(PartitionView(i -> "X", j -> "J"))) should be(false)
  }

  it should "process OR predicate" in {
    val predicate = OR(eqi, neqj)
    predicate.accept(new PartitionFilter(PartitionView(i -> "I", j -> "A"))) should be(true)
    predicate.accept(new PartitionFilter(PartitionView(i -> "I", j -> "J"))) should be(true)
    predicate.accept(new PartitionFilter(PartitionView(i -> "X", j -> "A"))) should be(true)
    predicate.accept(new PartitionFilter(PartitionView(i -> "X", j -> "J"))) should be(false)
  }

  it should "process NOT predicate" in {
    val predicate = NOT(eqi)
    predicate.accept(new PartitionFilter(PartitionView(i -> "I"))) should be(false)
    predicate.accept(new PartitionFilter(PartitionView(i -> "X"))) should be(true)
  }

  "PartitionFilterRewriter" should "rewrite case1 correctly" in {
    PartitionFilterRewriter.rewrite(case1, List.empty) should be(AssumeTrue)
    PartitionFilterRewriter.rewrite(case1, List(i, l)) should be(OR(eqi, ltl))
    PartitionFilterRewriter.rewrite(case1, List(i, j)) should be(AssumeTrue)
    PartitionFilterRewriter.rewrite(case1, List(k, l)) should be(AssumeTrue)
  }

  it should "rewrite case2 correctly" in {
    PartitionFilterRewriter.rewrite(case2, List.empty) should be(AssumeTrue)
    PartitionFilterRewriter.rewrite(case2, List(i, l)) should be(AssumeTrue)
    PartitionFilterRewriter.rewrite(case2, List(i, j)) should be(OR(udpi, neqj))
    PartitionFilterRewriter.rewrite(case2, List(k, l)) should be(OR(gtek, ltel))
  }

  it should "rewrite case3 correctly" in {
    PartitionFilterRewriter.rewrite(case3, List.empty) should be(AssumeTrue)
    PartitionFilterRewriter.rewrite(case3, List(i, j)) should be(NOT(AND(udpi, neqj)))
    PartitionFilterRewriter.rewrite(case3, List(i)) should be(NOT(udpi))
    PartitionFilterRewriter.rewrite(case3, List(j)) should be(NOT(neqj))
  }

  it should "rewrite case4 correctly" in {
    PartitionFilterRewriter.rewrite(case4, List.empty) should be(AssumeTrue)
    PartitionFilterRewriter.rewrite(case4, List(i, j)) should be(NOT(OR(eqi, neqj)))
    PartitionFilterRewriter.rewrite(case4, List(i)) should be(AssumeTrue)
    PartitionFilterRewriter.rewrite(case4, List(j)) should be(AssumeTrue)
  }

  "FilterRewriter" should "rewrite case1 correctly" in {
    FilterRewriter.rewrite(case1, PartitionView()) should be(case1)

    FilterRewriter.rewrite(case1, PartitionView(i -> "I", l -> "y")) should be(OR(neqj, gtk))
    FilterRewriter.rewrite(case1, PartitionView(i -> "I", j -> "X")) should be(IsTrue)
    FilterRewriter.rewrite(case1, PartitionView(k -> "b", l -> "X")) should be(IsTrue)

    FilterRewriter.rewrite(case1, PartitionView(i -> "X", l -> "z")) should be(IsFalse)
    FilterRewriter.rewrite(case1, PartitionView(i -> "X", j -> "J")) should be(AND(gtk, ltl))
    FilterRewriter.rewrite(case1, PartitionView(k -> "a", l -> "J")) should be(AND(eqi, neqj))
  }

  it should "rewrite case2 correctly" in {
    FilterRewriter.rewrite(case2, PartitionView()) should be(case2)

    FilterRewriter.rewrite(case2, PartitionView(i -> "ABC", l -> "y")) should be(IsTrue)
    FilterRewriter.rewrite(case2, PartitionView(i -> "ABC", j -> "X")) should be(OR(gtek, ltel))
    FilterRewriter.rewrite(case2, PartitionView(k -> "b", l -> "y")) should be(OR(udpi, neqj))

    FilterRewriter.rewrite(case2, PartitionView(i -> "abc", l -> "ż")) should be(AND(neqj, gtek))
    FilterRewriter.rewrite(case2, PartitionView(i -> "abc", j -> "J")) should be(IsFalse)
    FilterRewriter.rewrite(case2, PartitionView(k -> "A", l -> "ż")) should be(IsFalse)
  }

  it should "rewrite case3 correctly" in {
    FilterRewriter.rewrite(case3, PartitionView()) should be(case3)

    FilterRewriter.rewrite(case3, PartitionView(i -> "ABC", j -> "X")) should be(IsFalse)
    FilterRewriter.rewrite(case3, PartitionView(i -> "ABC")) should be(NOT(neqj))
    FilterRewriter.rewrite(case3, PartitionView(j -> "X")) should be(NOT(udpi))

    FilterRewriter.rewrite(case3, PartitionView(i -> "abc", j -> "J")) should be(IsTrue)
    FilterRewriter.rewrite(case3, PartitionView(i -> "abc")) should be(IsTrue)
    FilterRewriter.rewrite(case3, PartitionView(j -> "J")) should be(IsTrue)
  }

  it should "rewrite case4 correctly" in {
    NOT(OR(eqi, neqj))

    FilterRewriter.rewrite(case4, PartitionView()) should be(case4)

    FilterRewriter.rewrite(case4, PartitionView(i -> "I", j -> "X")) should be(IsFalse)
    FilterRewriter.rewrite(case4, PartitionView(i -> "I")) should be(IsFalse)
    FilterRewriter.rewrite(case4, PartitionView(j -> "X")) should be(IsFalse)

    FilterRewriter.rewrite(case4, PartitionView(i -> "X", j -> "J")) should be(IsTrue)
    FilterRewriter.rewrite(case4, PartitionView(i -> "X")) should be(NOT(neqj))
    FilterRewriter.rewrite(case4, PartitionView(j -> "J")) should be(NOT(eqi))
  }

}
