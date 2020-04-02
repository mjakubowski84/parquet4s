package com.github.mjakubowski84.parquet4s

import com.github.mjakubowski84.parquet4s.FilterRewriter.{IsFalse, IsTrue}
import com.github.mjakubowski84.parquet4s.PartitionedDirectory.PartitioningSchema
import org.apache.hadoop.fs.Path
import org.apache.parquet.filter2.compat.FilterCompat
import org.apache.parquet.filter2.predicate.Operators.Column
import org.apache.parquet.filter2.predicate.{FilterApi, FilterPredicate, Operators, UserDefinedPredicate}
import org.apache.parquet.io.api.Binary
import org.apache.parquet.schema.PrimitiveComparator
import org.slf4j.LoggerFactory


case class PartitionedPath(path: Path, partitions: List[(String, String)]) {
  lazy val partitionMap: Map[String, Binary] =
    partitions.map { case (name, value) => name -> Binary.fromString(value) }.toMap
}

object PartitionedDirectory {

  type PartitioningSchema = List[String]

  def apply(partitionedPaths: Iterable[PartitionedPath]): Either[Exception, PartitionedDirectory] = {
    val grouped = partitionedPaths.groupBy(_.partitions.map(_._1))
    Either.cond(
      test = grouped.size == 1,
      right = new PartitionedDirectory {
        override val schema: List[String] = grouped.head._1
        override val paths: Iterable[PartitionedPath] = partitionedPaths
      },
      left = new IllegalArgumentException(
        s"""Inconsistent partitioning.
        |Parquet files must live in leaf directories.
        |Every files must contain the same numbers of partitions.
        |Partition directories at the same level must have the same names.
        |Check following directories: ${grouped.values.map(_.head).mkString("\n\t", "\n\t", "")}
        |""".stripMargin)
    )
  }

}

trait PartitionedDirectory {
  def schema: PartitioningSchema
  def paths: Iterable[PartitionedPath]
}

private[parquet4s] object PartitionFilter {

  import PartitionFilterRewriter._

  private val logger = LoggerFactory.getLogger(this.getClass)

  private def debug(msg: => String): Unit = logger.debug(msg)

  def filter(filterPredicate: FilterPredicate, partitionedDirectory: PartitionedDirectory): Iterable[(FilterCompat.Filter, PartitionedPath)] = {
    val partitionFilterPredicate = PartitionFilterRewriter.rewrite(filterPredicate, partitionedDirectory.schema)
    debug(s"Using rewritten predicate to filter partition: $partitionFilterPredicate")
    partitionedDirectory
      .paths
      .filter {
        case _ if partitionFilterPredicate == AssumeTrue => true
        case partitionedPath => partitionFilterPredicate.accept(new PartitionFilter(partitionedPath))
      }
      .map(partitionedPath => (FilterRewriter.rewrite(filterPredicate, partitionedPath), partitionedPath))
      .flatMap {
        case (IsTrue, partitionedPath) =>
          debug(s"Filter $filterPredicate for $partitionedPath is always true, filter will be ignored")
          Some(FilterCompat.NOOP, partitionedPath)
        case (IsFalse, partitionedPath) =>
          debug(s"Filter $filterPredicate for $partitionedPath is always false, path won't be read")
          None
        case (rewritten, partitionedPath) =>
          debug(s"Filter $filterPredicate for $partitionedPath is rewritten to $rewritten")
          Some(FilterCompat.get(rewritten), partitionedPath)
      }
  }

}

private class PartitionFilter(partitionedPath: PartitionedPath) extends FilterPredicate.Visitor[Boolean] {

  override def visit[T <: Comparable[T]](eq: Operators.Eq[T]): Boolean =
    applyOperator(eq.getColumn, eq.getValue)(_ == 0)

  override def visit[T <: Comparable[T]](notEq: Operators.NotEq[T]): Boolean =
    applyOperator(notEq.getColumn, notEq.getValue)(_ != 0)

  override def visit[T <: Comparable[T]](lt: Operators.Lt[T]): Boolean =
    applyOperator(lt.getColumn, lt.getValue)(_ < 0)

  override def visit[T <: Comparable[T]](ltEq: Operators.LtEq[T]): Boolean =
    applyOperator(ltEq.getColumn, ltEq.getValue)(_ <= 0)

  override def visit[T <: Comparable[T]](gt: Operators.Gt[T]): Boolean =
    applyOperator(gt.getColumn, gt.getValue)(_ > 0)

  override def visit[T <: Comparable[T]](gtEq: Operators.GtEq[T]): Boolean =
    applyOperator(gtEq.getColumn, gtEq.getValue)(_ >= 0)

  override def visit(and: Operators.And): Boolean =
    and.getLeft.accept(this) && and.getRight.accept(this)

  override def visit(or: Operators.Or): Boolean =
    or.getLeft.accept(this) || or.getRight.accept(this)

  override def visit(not: Operators.Not): Boolean = !not.getPredicate.accept(this)

  override def visit[T <: Comparable[T], U <: UserDefinedPredicate[T]](udp: Operators.UserDefined[T, U]): Boolean =
    applyOperator(udp.getColumn)(partitionValue => udp.getUserDefinedPredicate.keep(partitionValue.asInstanceOf[T]))

  override def visit[T <: Comparable[T], U <: UserDefinedPredicate[T]](udp: Operators.LogicalNotUserDefined[T, U]): Boolean =
    applyOperator(udp.getUserDefined.getColumn) { partitionValue =>
      udp.getUserDefined.getUserDefinedPredicate.keep(partitionValue.asInstanceOf[T])
    }

  private def applyOperator[T <: Comparable[T]](column: Column[T])(op: Binary => Boolean) = {
    val columnPath = column.getColumnPath.toDotString
    val filterType = column.getColumnType
    partitionedPath.partitionMap.get(columnPath) match {
      case None =>
        false
      case Some(partitionValue) if filterType == classOf[Binary] =>
        op(partitionValue)
      case _ =>
        throw new IllegalArgumentException(
          s"Filter type does not match schema, column $columnPath is Binary String while filter is $filterType"
        )
    }
  }

  private def applyOperator[T <: Comparable[T]](column: Column[T], value: T)
                                               (op: Int => Boolean): Boolean =
    applyOperator(column)(partitionValue => op(compareBinaries(partitionValue, value.asInstanceOf[Binary])))

  private def compareBinaries(x: Binary, y: Binary): Int =
    PrimitiveComparator.UNSIGNED_LEXICOGRAPHICAL_BINARY_COMPARATOR.compare(x, y)

}

private[parquet4s] object PartitionFilterRewriter {

  case object AssumeTrue extends FilterPredicate {
    override def accept[R](visitor: FilterPredicate.Visitor[R]): R = throw new UnsupportedOperationException
  }

  def rewrite(filterPredicate: FilterPredicate, schema: PartitioningSchema): FilterPredicate =
    filterPredicate.accept(new PartitionFilterRewriter(schema))

}

private class PartitionFilterRewriter(schema: PartitioningSchema)
    extends FilterPredicate.Visitor[FilterPredicate] {

  import PartitionFilterRewriter._

  private def isPartitionFilter(column: Column[_]): Boolean =
    schema.contains(column.getColumnPath.toDotString)

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

  override def visit(not: Operators.Not): FilterPredicate =
    not.getPredicate.accept(this) match {
      case AssumeTrue => AssumeTrue
      case other => FilterApi.not(other)
    }

  override def visit[T <: Comparable[T], U <: UserDefinedPredicate[T]](udp: Operators.UserDefined[T, U]): FilterPredicate =
    if (isPartitionFilter(udp.getColumn)) udp
    else AssumeTrue

  override def visit[T <: Comparable[T], U <: UserDefinedPredicate[T]](udp: Operators.LogicalNotUserDefined[T, U]): FilterPredicate =
    if (isPartitionFilter(udp.getUserDefined.getColumn)) udp
    else AssumeTrue
}

private[parquet4s] object FilterRewriter {

  case object IsTrue extends FilterPredicate {
    override def accept[R](visitor: FilterPredicate.Visitor[R]): R = throw new UnsupportedOperationException
  }

  case object IsFalse extends FilterPredicate {
    override def accept[R](visitor: FilterPredicate.Visitor[R]): R = throw new UnsupportedOperationException
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
      case (left, right) => FilterApi.and(left, right)
    }

  override def visit(or: Operators.Or): FilterPredicate =
    (or.getLeft.accept(this), or.getRight.accept(this)) match {
      case (IsTrue, _) => IsTrue
      case (_, IsTrue) => IsTrue
      case (IsFalse, IsFalse) => IsFalse
      case (IsFalse, right) => right
      case (left, IsFalse) => left
      case (left, right) => FilterApi.or(left, right)
    }

  override def visit(not: Operators.Not): FilterPredicate =
    not.getPredicate.accept(this) match {
      case IsTrue => IsFalse
      case IsFalse => IsTrue
      case predicate => FilterApi.not(predicate)
    }

  override def visit[T <: Comparable[T], U <: UserDefinedPredicate[T]](udp: Operators.UserDefined[T, U]): FilterPredicate =
    if (isPartitionFilter(udp.getColumn)) evaluate(udp)
    else udp

  override def visit[T <: Comparable[T], U <: UserDefinedPredicate[T]](udp: Operators.LogicalNotUserDefined[T, U]): FilterPredicate =
    if (isPartitionFilter(udp.getUserDefined.getColumn)) evaluate(udp)
    else udp

}
