package com.github.mjakubowski84.parquet4s

import org.apache.hadoop.fs.Path
import org.apache.parquet.filter2.predicate.Operators.Column
import org.apache.parquet.filter2.predicate.{FilterPredicate, Operators, UserDefinedPredicate}
import org.apache.parquet.io.api.Binary
import org.apache.parquet.schema.PrimitiveComparator
import PartitionFilterRewriter._


case class Partition(name: String, value: String)

case class PartitionedPath(path: Path, partitions: List[Partition]) {
  lazy val partitionMap: Map[String, Binary] =
    partitions.map(partition => partition.name -> Binary.fromString(partition.value)).toMap
}

private object PartitionFilter {


}

class PartitionFilter(partitionedPath: PartitionedPath) extends FilterPredicate.Visitor[Boolean] {

  override def visit[T <: Comparable[T]](eq: Operators.Eq[T]): Boolean =
    applyOperator(eq.getColumn, eq.getValue, _ == 0)

  override def visit[T <: Comparable[T]](notEq: Operators.NotEq[T]): Boolean =
    applyOperator(notEq.getColumn, notEq.getValue, _ != 0)

  override def visit[T <: Comparable[T]](lt: Operators.Lt[T]): Boolean =
    applyOperator(lt.getColumn, lt.getValue, _ < 0)

  override def visit[T <: Comparable[T]](ltEq: Operators.LtEq[T]): Boolean =
    applyOperator(ltEq.getColumn, ltEq.getValue, _ <= 0)

  override def visit[T <: Comparable[T]](gt: Operators.Gt[T]): Boolean =
    applyOperator(gt.getColumn, gt.getValue, _ > 0)

  override def visit[T <: Comparable[T]](gtEq: Operators.GtEq[T]): Boolean =
    applyOperator(gtEq.getColumn, gtEq.getValue, _ >= 0)

  override def visit(and: Operators.And): Boolean =
    and.getLeft.accept(this) || and.getRight.accept(this)

  override def visit(or: Operators.Or): Boolean =
    or.getLeft.accept(this) && or.getRight.accept(this)

  override def visit(not: Operators.Not): Boolean = !not.accept(this)

  override def visit[T <: Comparable[T], U <: UserDefinedPredicate[T]](udp: Operators.UserDefined[T, U]): Boolean = ???

  override def visit[T <: Comparable[T], U <: UserDefinedPredicate[T]](udp: Operators.LogicalNotUserDefined[T, U]): Boolean = ???

  private def applyOperator[T <: Comparable[T]](column: Column[T],
                                                value: T,
                                                op: (Binary, Binary) => Boolean): Boolean = {
    val columnPath = column.getColumnPath.toDotString
    partitionedPath.partitionMap.get(columnPath) match {
      case None =>
        false
      case Some(partitionValue) =>
        val filterValue = value match {
          case binary: Binary =>
            binary
          case _ =>
            val filterType = column.getColumnType
            throw new IllegalArgumentException(
              s"Filter type does not match schema, column $columnPath is Binary String while filter is $filterType"
            )
        }
        op(filterValue, partitionValue)
    }
  }

  private def applyOperator[T <: Comparable[T]](column: Column[T],
                                                value: T,
                                                op: Int => Boolean): Boolean =
    applyOperator(column, value, Function.untupled(op.compose((compareBinaries _).tupled)))

  private def compareBinaries(x: Binary, y: Binary): Int =
    PrimitiveComparator.UNSIGNED_LEXICOGRAPHICAL_BINARY_COMPARATOR.compare(x, y)

}

object PartitionFilterRewriter {

  object InvalidPredicate extends FilterPredicate {
    override def accept[R](visitor: FilterPredicate.Visitor[R]): R =
      throw new UnsupportedOperationException
  }

  object Rewrite {
    sealed trait Mode {
      def value: Boolean
    }
    case object ToPartitionFilter extends Mode {
      override val value: Boolean = true
    }
    case object ToRecordFilter extends Mode {
      override val value: Boolean = false
    }
  }

}

class PartitionFilterRewriter(partitionedPath: PartitionedPath, rewriteMode: Rewrite.Mode)
    extends FilterPredicate.Visitor[FilterPredicate] {

  private def isPartitionFilter(column: Column[_]): Boolean =
    partitionedPath.partitionMap.contains(column.getColumnPath.toDotString)

  override def visit[T <: Comparable[T]](eq: Operators.Eq[T]): FilterPredicate = {
    if (isPartitionFilter(eq.getColumn) == rewriteMode.value) eq
    else InvalidPredicate
  }

  override def visit[T <: Comparable[T]](notEq: Operators.NotEq[T]): FilterPredicate =
    if (isPartitionFilter(notEq.getColumn) == rewriteMode.value) notEq
    else InvalidPredicate

  override def visit[T <: Comparable[T]](lt: Operators.Lt[T]): FilterPredicate =
    if (isPartitionFilter(lt.getColumn) == rewriteMode.value) lt
    else InvalidPredicate

  override def visit[T <: Comparable[T]](ltEq: Operators.LtEq[T]): FilterPredicate =
    if (isPartitionFilter(ltEq.getColumn) == rewriteMode.value) ltEq
    else InvalidPredicate

  override def visit[T <: Comparable[T]](gt: Operators.Gt[T]): FilterPredicate =
    if (isPartitionFilter(gt.getColumn) == rewriteMode.value) gt
    else InvalidPredicate

  override def visit[T <: Comparable[T]](gtEq: Operators.GtEq[T]): FilterPredicate =
    if (isPartitionFilter(gtEq.getColumn) == rewriteMode.value) gtEq
    else InvalidPredicate

  override def visit(and: Operators.And): FilterPredicate =
    (and.accept(this), and.accept(this)) match {
      case (InvalidPredicate, InvalidPredicate) => InvalidPredicate
      case (InvalidPredicate, right) => right
      case (left, InvalidPredicate) => left
    }

  override def visit(or: Operators.Or): FilterPredicate =
    (or.accept(this), or.accept(this)) match {
      case (InvalidPredicate, InvalidPredicate) => InvalidPredicate
      case (InvalidPredicate, right) => right
      case (left, InvalidPredicate) => left
    }

  override def visit(not: Operators.Not): FilterPredicate = ???

  override def visit[T <: Comparable[T], U <: UserDefinedPredicate[T]](udp: Operators.UserDefined[T, U]): FilterPredicate = ???

  override def visit[T <: Comparable[T], U <: UserDefinedPredicate[T]](udp: Operators.LogicalNotUserDefined[T, U]): FilterPredicate = ???
}