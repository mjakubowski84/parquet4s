package com.github.mjakubowski84.parquet4s

import com.github.mjakubowski84.parquet4s.PartitionFilterRewriter.AssumeTrue
import com.github.mjakubowski84.parquet4s.PartitionFilterRewriterSpec.IsUppercase
import org.apache.commons.lang3.StringUtils
import org.apache.parquet.filter2.predicate.FilterApi.{and => AND, eq => EQ, gt => GT, gtEq => GTE, lt => LT, ltEq => LTE, not => NOT, notEq => NEQ, or => OR, userDefined => UDP, _}
import org.apache.parquet.filter2.predicate.{FilterPredicate, Operators, Statistics, UserDefinedPredicate}
import org.apache.parquet.io.api.Binary
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

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

  "PartitionFilterRewriter" should "rewrite case1 correctly" in {
    PartitionFilterRewriter.rewrite(case1, List.empty) should be(AssumeTrue)
    PartitionFilterRewriter.rewrite(case1, List(i, l)) should be(OR(eqi, ltl))
    PartitionFilterRewriter.rewrite(case1, List(i, j)) should be(AssumeTrue)
    PartitionFilterRewriter.rewrite(case1, List(k, l)) should be(AssumeTrue)
  }

  it should "rewrite case2 correctly" in {
    PartitionFilterRewriter.rewrite(case2, List.empty) should be(AssumeTrue)
    PartitionFilterRewriter.rewrite(case2, List(i, l)) should be(AssumeTrue)
    PartitionFilterRewriter.rewrite(case2, List(i, j)) should be(OR(udpi, udpj))
    PartitionFilterRewriter.rewrite(case2, List(k, l)) should be(OR(gtek, ltel))
  }

  it should "rewrite case3 correctly" in {
    PartitionFilterRewriter.rewrite(case3, List.empty) should be(AssumeTrue)
    PartitionFilterRewriter.rewrite(case3, List(i, j)) should be(NOT(AND(udpi, udpj)))
    PartitionFilterRewriter.rewrite(case3, List(i)) should be(NOT(udpi))
    PartitionFilterRewriter.rewrite(case3, List(j)) should be(NOT(udpj))
  }

  it should "rewrite case4 correctly" in {
    PartitionFilterRewriter.rewrite(case4, List.empty) should be(AssumeTrue)
    PartitionFilterRewriter.rewrite(case4, List(i, j)) should be(NOT(OR(eqi, neqj)))
    PartitionFilterRewriter.rewrite(case4, List(i)) should be(AssumeTrue)
    PartitionFilterRewriter.rewrite(case4, List(j)) should be(AssumeTrue)
  }

}
