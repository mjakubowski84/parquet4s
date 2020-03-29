package com.github.mjakubowski84.parquet4s

import org.apache.hadoop.fs.Path
import org.apache.parquet.filter2.predicate.Operators.Column
import org.apache.parquet.filter2.predicate.{FilterApi, FilterPredicate, Operators, UserDefinedPredicate}
import org.apache.parquet.io.api.Binary
import org.apache.parquet.schema.PrimitiveComparator


case class Partition(name: String, value: String)

case class PartitionedPath(path: Path, partitions: List[Partition]) {
  lazy val partitionMap: Map[String, Binary] =
    partitions.map(partition => partition.name -> Binary.fromString(partition.value)).toMap
}

private object PartitionFilter {

  import PartitionFilterRewriter._

  def filter(filterPredicate: FilterPredicate)(partitionedPath: PartitionedPath): Boolean =
    PartitionFilterRewriter.rewrite(filterPredicate, partitionedPath) match {
      case AssumeTrue =>
        println("Rewritten predicate is assumed to be always true")
        true
      case rewritten =>
        println(s"Using rewritten predicate to filter partition: $rewritten")
        rewritten.accept(new PartitionFilter(partitionedPath))
    }

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

  case object AssumeTrue extends FilterPredicate {
    override def accept[R](visitor: FilterPredicate.Visitor[R]): R =
      throw new UnsupportedOperationException
  }

  def rewrite(filterPredicate: FilterPredicate, partitionedPath: PartitionedPath): FilterPredicate =
    filterPredicate.accept(new PartitionFilterRewriter(partitionedPath))

}

private class PartitionFilterRewriter(partitionedPath: PartitionedPath)
    extends FilterPredicate.Visitor[FilterPredicate] {

  import PartitionFilterRewriter._

  private def isPartitionFilter(column: Column[_]): Boolean =
    partitionedPath.partitionMap.contains(column.getColumnPath.toDotString)

  override def visit[T <: Comparable[T]](eq: Operators.Eq[T]): FilterPredicate = {
    if (isPartitionFilter(eq.getColumn)) eq
    else AssumeTrue
  }

  override def visit[T <: Comparable[T]](notEq: Operators.NotEq[T]): FilterPredicate =
    if (isPartitionFilter(notEq.getColumn)) notEq
    else AssumeTrue

  override def visit[T <: Comparable[T]](lt: Operators.Lt[T]): FilterPredicate =
    if (isPartitionFilter(lt.getColumn)) lt
    else AssumeTrue

  override def visit[T <: Comparable[T]](ltEq: Operators.LtEq[T]): FilterPredicate =
    if (isPartitionFilter(ltEq.getColumn)) ltEq
    else AssumeTrue

  override def visit[T <: Comparable[T]](gt: Operators.Gt[T]): FilterPredicate =
    if (isPartitionFilter(gt.getColumn)) gt
    else AssumeTrue

  override def visit[T <: Comparable[T]](gtEq: Operators.GtEq[T]): FilterPredicate =
    if (isPartitionFilter(gtEq.getColumn)) gtEq
    else AssumeTrue

  override def visit(and: Operators.And): FilterPredicate =
    (and.getLeft.accept(this), and.getRight.accept(this)) match {
      case (AssumeTrue, AssumeTrue) => AssumeTrue
      case (AssumeTrue, right) => right
      case (left, AssumeTrue) => left
      case (left, right) => FilterApi.and(left, right)
    }

  override def visit(or: Operators.Or): FilterPredicate =
    (or.getLeft.accept(this), or.getRight.accept(this)) match {
      case (AssumeTrue, AssumeTrue) => AssumeTrue
      case (AssumeTrue, _) => AssumeTrue
      case (_, AssumeTrue) => AssumeTrue
      case (left, right) => FilterApi.or(left, right)
    }

  override def visit(not: Operators.Not): FilterPredicate = {
    if (not.getPredicate == AssumeTrue) AssumeTrue // TODO AssumeFalse?
    else not
  }

  override def visit[T <: Comparable[T], U <: UserDefinedPredicate[T]](udp: Operators.UserDefined[T, U]): FilterPredicate =
    if (isPartitionFilter(udp.getColumn)) udp
    else AssumeTrue

  override def visit[T <: Comparable[T], U <: UserDefinedPredicate[T]](udp: Operators.LogicalNotUserDefined[T, U]): FilterPredicate =
    if (isPartitionFilter(udp.getUserDefined.getColumn)) udp
    else AssumeTrue
}

object FilterRewriter {

  case object IsTrue extends FilterPredicate {
    override def accept[R](visitor: FilterPredicate.Visitor[R]): R =
      throw new UnsupportedOperationException
  }

  case object IsFalse extends FilterPredicate {
    override def accept[R](visitor: FilterPredicate.Visitor[R]): R =
      throw new UnsupportedOperationException
  }

  def rewrite(filterPredicate: FilterPredicate, partitionedPath: PartitionedPath): FilterPredicate =
    filterPredicate.accept(new FilterRewriter(partitionedPath))

}

private class FilterRewriter(partitionedPath: PartitionedPath)
    extends FilterPredicate.Visitor[FilterPredicate] {

  import FilterRewriter._

  private def isPartitionFilter(column: Column[_]): Boolean =
    partitionedPath.partitionMap.contains(column.getColumnPath.toDotString)

  private def evaluate(filterPredicate: FilterPredicate): FilterPredicate =
    if (filterPredicate.accept(new PartitionFilter(partitionedPath))) IsTrue
    else IsFalse

  override def visit[T <: Comparable[T]](eq: Operators.Eq[T]): FilterPredicate =
    if (isPartitionFilter(eq.getColumn)) evaluate(eq)
    else eq

  override def visit[T <: Comparable[T]](notEq: Operators.NotEq[T]): FilterPredicate =
    if (isPartitionFilter(notEq.getColumn)) evaluate(notEq)
    else notEq

  override def visit[T <: Comparable[T]](lt: Operators.Lt[T]): FilterPredicate =
    if (isPartitionFilter(lt.getColumn)) evaluate(lt)
    else lt

  override def visit[T <: Comparable[T]](ltEq: Operators.LtEq[T]): FilterPredicate =
    if (isPartitionFilter(ltEq.getColumn)) evaluate(ltEq)
    else ltEq

  override def visit[T <: Comparable[T]](gt: Operators.Gt[T]): FilterPredicate =
    if (isPartitionFilter(gt.getColumn)) evaluate(gt)
    else gt

  override def visit[T <: Comparable[T]](gtEq: Operators.GtEq[T]): FilterPredicate =
    if (isPartitionFilter(gtEq.getColumn)) evaluate(gtEq)
    else gtEq

  override def visit(and: Operators.And): FilterPredicate =
    (and.getLeft.accept(this), and.getRight.accept(this)) match {
      case (IsFalse, _) => IsFalse
      case (_, IsFalse) => IsFalse
      case (IsTrue, IsTrue) => IsTrue
      case (IsTrue, right) => right
      case (left, IsTrue) => left
      case (_, _) => and
    }

  override def visit(or: Operators.Or): FilterPredicate =
    (or.getLeft.accept(this), or.getRight.accept(this)) match {
      case (IsTrue, _) => IsTrue
      case (_, IsTrue) => IsTrue
      case (IsFalse, IsFalse) => IsFalse
      case (IsFalse, right) => right
      case (left, IsFalse) => left
      case (_, _) => or
    }

  override def visit(not: Operators.Not): FilterPredicate =
    not.getPredicate match {
      case IsTrue => IsFalse
      case IsFalse => IsTrue
      case _ => not
    }

  override def visit[T <: Comparable[T], U <: UserDefinedPredicate[T]](udp: Operators.UserDefined[T, U]): FilterPredicate =
    if (isPartitionFilter(udp.getColumn)) evaluate(udp)
    else udp

  override def visit[T <: Comparable[T], U <: UserDefinedPredicate[T]](udp: Operators.LogicalNotUserDefined[T, U]): FilterPredicate =
    if (isPartitionFilter(udp.getUserDefined.getColumn)) evaluate(udp)
    else udp

}