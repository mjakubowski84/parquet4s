package com.github.mjakubowski84.parquet4s

import com.github.mjakubowski84.parquet4s.PartitionFilterRewriter.AssumeTrue
import com.github.mjakubowski84.parquet4s.PartitionFilterRewriterSpec.IsUppercase
import org.apache.commons.lang3.StringUtils
import org.apache.hadoop.fs.Path
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.apache.parquet.filter2.predicate.FilterApi.{and => AND, eq => EQ, gt => GT, gtEq => GTE, lt => LT, ltEq => LTE, not => NOT, notEq => NEQ, or => OR, userDefined => UDP, _}
import org.apache.parquet.filter2.predicate.{FilterPredicate, Operators, Statistics, UserDefinedPredicate}
import org.apache.parquet.io.api.Binary

object PartitionFilterRewriterSpec {

  class IsUppercase extends UserDefinedPredicate[Binary] with Serializable {
    override def keep(value: Binary): Boolean = StringUtils.isAllUpperCase(value.toStringUsingUTF8)
    override def canDrop(statistics: Statistics[Binary]): Boolean = false
    override def inverseCanDrop(statistics: Statistics[Binary]): Boolean = false
    override def toString: String = "is_uppercase"
  }


}

class PartitionFilterRewriterSpec extends AnyFlatSpec with Matchers {

  val (i, j, k, l) = ("i", "j", "k", "l")
  val (colI, colJ, colK, colL) = (binaryColumn(i), binaryColumn(j), binaryColumn(k), binaryColumn(l))

  val eqi: Operators.Eq[Binary] = EQ(colI, Binary.fromString("I"))
  val neqj: Operators.NotEq[Binary] = NEQ(colJ, Binary.fromString("J"))
  val gtk: Operators.Gt[Binary] = GT(colK, Binary.fromString("a"))
  val ltl: Operators.Lt[Binary] = LT(colL, Binary.fromString("z"))
  val gtek: Operators.GtEq[Binary] = GTE(colK, Binary.fromString("a"))
  val ltel: Operators.LtEq[Binary] = LTE(colL, Binary.fromString("z"))
  val udpi: Operators.UserDefined[Binary, IsUppercase] = UDP(colI, new IsUppercase)
  val udpj: Operators.UserDefined[Binary, IsUppercase] = UDP(colJ, classOf[IsUppercase])

  val case1: FilterPredicate = OR(AND(eqi, neqj), AND(gtk, ltl))
  val case2: FilterPredicate = AND(OR(udpi, udpj), OR(gtek, ltel))
  val case3: FilterPredicate = NOT(AND(udpi, udpj))
  val case4: FilterPredicate = NOT(OR(eqi, neqj))

  def partitionedPath(partitions: (String, String)*): PartitionedPath =
    PartitionedPath(new Path("path"), partitions.toList)

  "PartitionFilterRewriter" should "rewrite case1 correctly" in {
    PartitionFilterRewriter.rewrite(case1, partitionedPath()) should be(AssumeTrue)
    PartitionFilterRewriter.rewrite(case1, partitionedPath(i -> "X", l -> "Y")) should be(OR(eqi, ltl))
    PartitionFilterRewriter.rewrite(case1, partitionedPath(i -> "X", j -> "Y")) should be(AssumeTrue)
    PartitionFilterRewriter.rewrite(case1, partitionedPath(k -> "X", l -> "Y")) should be(AssumeTrue)
  }

  it should "rewrite case2 correctly" in {
    PartitionFilterRewriter.rewrite(case2, partitionedPath()) should be(AssumeTrue)
    PartitionFilterRewriter.rewrite(case2, partitionedPath(i -> "X", l -> "Y")) should be(AssumeTrue)
    PartitionFilterRewriter.rewrite(case2, partitionedPath(i -> "X", j -> "Y")) should be(OR(udpi, udpj))
    PartitionFilterRewriter.rewrite(case2, partitionedPath(k -> "X", l -> "Y")) should be(OR(gtek, ltel))
  }

  it should "rewrite case3 correctly" in {
    PartitionFilterRewriter.rewrite(case3, partitionedPath()) should be(AssumeTrue)
    PartitionFilterRewriter.rewrite(case3, partitionedPath(i -> "X", j -> "Y")) should be(NOT(AND(udpi, udpj)))
    PartitionFilterRewriter.rewrite(case3, partitionedPath(i -> "X")) should be(NOT(udpi))
    PartitionFilterRewriter.rewrite(case3, partitionedPath(j -> "X")) should be(NOT(udpj))
  }

  it should "rewrite case4 correctly" in {
    PartitionFilterRewriter.rewrite(case4, partitionedPath()) should be(AssumeTrue)
    PartitionFilterRewriter.rewrite(case4, partitionedPath(i -> "X", j -> "Y")) should be(NOT(OR(eqi, neqj)))
    PartitionFilterRewriter.rewrite(case4, partitionedPath(i -> "X")) should be(AssumeTrue)
    PartitionFilterRewriter.rewrite(case4, partitionedPath(j -> "X")) should be(AssumeTrue)
  }
  
}
