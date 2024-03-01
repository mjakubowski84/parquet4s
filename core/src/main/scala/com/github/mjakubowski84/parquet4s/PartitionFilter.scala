package com.github.mjakubowski84.parquet4s

import com.github.mjakubowski84.parquet4s.PartitionedDirectory.PartitioningSchema
import com.github.mjakubowski84.parquet4s.PartitionedPath.Partition
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileStatus
import org.apache.parquet.filter2.predicate.Operators.Column
import org.apache.parquet.filter2.predicate.{FilterApi, FilterPredicate, Operators, UserDefinedPredicate}
import org.apache.parquet.hadoop.util.HadoopInputFile
import org.apache.parquet.io.InputFile
import org.apache.parquet.io.api.Binary
import org.apache.parquet.schema.PrimitiveComparator
import org.slf4j.LoggerFactory

object PartitionedPath {

  type Partition = (ColumnPath, String)

  def apply(
      fileStatus: FileStatus,
      configuration: Configuration,
      partitions: List[Partition],
      filterPredicateOpt: Option[FilterPredicate]
  ): PartitionedPath =
    apply(HadoopInputFile.fromStatus(fileStatus, configuration), partitions, filterPredicateOpt)

  def apply(
      path: Path,
      configuration: Configuration,
      partitions: List[Partition],
      filterPredicateOpt: Option[FilterPredicate]
  ): PartitionedPath =
    apply(HadoopInputFile.fromPath(path.toHadoop, configuration), partitions, filterPredicateOpt)

  def apply(
      hadoopInputFile: HadoopInputFile,
      partitions: List[Partition],
      filterPredicateOpt: Option[FilterPredicate]
  ): PartitionedPath =
    apply(
      path               = Path(hadoopInputFile.getPath),
      inputFile          = hadoopInputFile,
      partitions         = partitions,
      filterPredicateOpt = filterPredicateOpt
    )

  private def apply(
      path: Path,
      inputFile: HadoopInputFile,
      partitions: List[Partition],
      filterPredicateOpt: Option[FilterPredicate]
  ): PartitionedPath =
    new PartitionedPathImpl(
      path               = path,
      inputFile          = inputFile,
      partitions         = partitions.map { case (name, value) => (name, Binary.fromString(value)) },
      filterPredicateOpt = filterPredicateOpt
    )

}

/** Represents a path in file system that leads to a leaf directory containing Parquet files. Path can be partitioned,
  * that is, parent directories may be named in a following manner: {{{partition_name=partition_value}}}
  */
trait PartitionedPath {

  /** @return
    *   file system path
    */
  def path: Path

  /** @return
    *   file for reading
    */
  def inputFile: InputFile

  /** @return
    *   list of partition names
    */
  @deprecated(message = "Use view.schema instead", since = "2.17.0")
  def schema: PartitionedDirectory.PartitioningSchema

  /** @return
    *   value of given partition or None if there is no such partition in that path
    */
  @deprecated(message = "Use view.value instead", since = "2.17.0")
  def value(columnPath: ColumnPath): Option[Binary]

  /** @return
    *   list of all partitions and their values in that path
    */
  def partitions: List[(ColumnPath, Binary)]

  /** @return
    *   Rewritten filter predicate, which is a simplified by removal of references to partition fields. To be used to
    *   filter files in the partition.
    */
  def filterPredicateOpt: Option[FilterPredicate]

  /** View over partition data.
    */
  def view: PartitionView

}

object PartitionView {
  def apply(partitions: List[Partition]): PartitionView =
    new PartitionView(partitions.map { case (name, value) => (name, Binary.fromString(value)) })
  def apply(partitions: Partition*): PartitionView = PartitionView(partitions.toList)
}

/** View over partition data.
  *
  * @param partitions
  */
class PartitionView(partitions: List[(ColumnPath, Binary)]) {
  private lazy val partitionMap: Map[ColumnPath, Binary] = partitions
    .foldLeft(Map.newBuilder[ColumnPath, Binary])(_ += _)
    .result()

  /** The list of all partition fields in given [[PartitionedDirectory]] .
    */
  lazy val schema: PartitionedDirectory.PartitioningSchema = partitions.map(_._1)

  /** @param columnPath
    *   partition name
    * @return
    *   a value of given partion field
    */
  def value(columnPath: ColumnPath): Option[Binary] = partitionMap.get(columnPath)
}

private class PartitionedPathImpl(
    override val path: Path,
    override val inputFile: InputFile,
    override val partitions: List[(ColumnPath, Binary)],
    override val filterPredicateOpt: Option[FilterPredicate]
) extends PartitionedPath {

  override val view: PartitionView = new PartitionView(partitions)

  override val schema: PartitioningSchema = view.schema

  override def value(columnPath: ColumnPath): Option[Binary] = view.value(columnPath)

  override lazy val toString: String = path.toString

  def canEqual(other: Any): Boolean = other.isInstanceOf[PartitionedPathImpl]

  override def equals(other: Any): Boolean = other match {
    case that: PartitionedPathImpl =>
      (that canEqual this) &&
      path == that.path &&
      partitions == that.partitions &&
      filterPredicateOpt == that.filterPredicateOpt
    case _ => false
  }

  override def hashCode(): Int = {
    val state = Seq(path, partitions, filterPredicateOpt)
    state.map(_.hashCode()).foldLeft(0)((a, b) => 31 * a + b)
  }
}

object PartitionedDirectory {

  type PartitioningSchema = List[ColumnPath]

  private[parquet4s] def failed(invalidPaths: Iterable[Path]): Left[Exception, PartitionedDirectory] =
    Left(
      new IllegalArgumentException(
        s"""Inconsistent partitioning.
         |Parquet files must live in leaf directories.
         |Every files must contain the same numbers of partitions.
         |Partition directories at the same level must have the same names.
         |Check following directories: ${invalidPaths.mkString("\n\t", "\n\t", "")}
         |""".stripMargin
      )
    )

  def apply(partitionedPaths: Iterable[PartitionedPath]): Either[Exception, PartitionedDirectory] = {
    val grouped = partitionedPaths.groupBy(_.view.schema)
    if (grouped.size <= 1)
      Right(
        new PartitionedDirectory {
          override val schema: PartitioningSchema       = grouped.headOption.map(_._1).getOrElse(List.empty)
          override val paths: Iterable[PartitionedPath] = partitionedPaths
        }
      )
    else failed(grouped.values.flatMap(_.headOption).map(_.path))
  }

}

/** Represents a directory tree containing Parquet files in the leafs. Node directories can be partitioned, that is,
  * they can be named in a following manner: {{{partition_name=partition_value}}} Depth of the tree is the same for each
  * leaf. Each leaf has the same path to the root.
  */
trait PartitionedDirectory {

  /** @return
    *   list of partition names used by each [[PartitionedPath]] in the directory tree.
    */
  def schema: PartitioningSchema

  /** @return
    *   all [[PartitionedPath]]s belonging to this directory
    */
  def paths: Iterable[PartitionedPath]
}

private[parquet4s] object PartitionFilter {

  import PartitionFilterRewriter.*

  private val logger = LoggerFactory.getLogger(this.getClass)

  private def debug(msg: => String): Unit = if (logger.isDebugEnabled()) logger.debug(msg)

  /** @param filterPredicate
    *   filter to be applied to each path
    * @param commonPartitions
    *   partitions resolved from parent directories
    * @param partitionedPaths
    *   partition paths to be filtered
    * @return
    *   partition paths that meet the filter
    */
  def filterPartitionPaths(
      filterPredicate: FilterPredicate,
      commonPartitions: List[Partition],
      partitionedPaths: Vector[(Path, Partition)]
  ): Vector[(Path, List[Partition])] =
    partitionedPaths.flatMap { case (path, partition) =>
      // TODO maybe using vector is better (we are appending here!)
      val partitions               = commonPartitions :+ partition
      val partitioningSchema       = partitions.map { case (columnPath, _) => columnPath }
      val partitionFilterPredicate = PartitionFilterRewriter.rewrite(filterPredicate, partitioningSchema)
      if (partitionFilterPredicate == AssumeTrue) {
        Some(path, partitions)
      } else {
        if (partitionFilterPredicate.accept(new PartitionFilter(PartitionView(partitions)))) {
          Some(path, partitions)
        } else {
          debug(s"Skipping $path as it doesn't match filter $filterPredicate")
          None
        }
      }
    }

}

private class PartitionFilter(partitionView: PartitionView) extends FilterPredicate.Visitor[Boolean] {

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

  override def visit[T <: Comparable[T], U <: UserDefinedPredicate[T]](
      udp: Operators.LogicalNotUserDefined[T, U]
  ): Boolean =
    applyOperator(udp.getUserDefined.getColumn) { partitionValue =>
      udp.getUserDefined.getUserDefinedPredicate.keep(partitionValue.asInstanceOf[T])
    }

  // TODO support In and NotIn

  private def applyOperator[T <: Comparable[T]](column: Column[T])(op: Binary => Boolean): Boolean = {
    val columnPath = ColumnPath(column.getColumnPath)
    val filterType = column.getColumnType
    partitionView.value(columnPath) match {
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

  private def applyOperator[T <: Comparable[T]](column: Column[T], value: T)(op: Int => Boolean): Boolean =
    applyOperator(column)(partitionValue => op(compareBinaries(partitionValue, value.asInstanceOf[Binary])))

  private def compareBinaries(x: Binary, y: Binary): Int =
    PrimitiveComparator.UNSIGNED_LEXICOGRAPHICAL_BINARY_COMPARATOR.compare(x, y)

}

private[parquet4s] object PartitionFilterRewriter {

  case object AssumeTrue extends FilterPredicate {
    override def accept[R](visitor: FilterPredicate.Visitor[R]): R = throw new UnsupportedOperationException
  }

  /** Rewrites given filter predicate so that it can be used to filter partitioned path. Only arguments matching
    * partition names are left, all other predicates are assumed to be true.
    * @param filterPredicate
    *   predicate to be rewritten
    * @param schema
    *   list of partition names
    * @return
    *   rewritten filter predicate.
    */
  def rewrite(filterPredicate: FilterPredicate, schema: PartitioningSchema): FilterPredicate =
    filterPredicate.accept(new PartitionFilterRewriter(schema))

}

private class PartitionFilterRewriter(schema: PartitioningSchema) extends FilterPredicate.Visitor[FilterPredicate] {

  import PartitionFilterRewriter.*

  private def isPartitionFilter(column: Column[?]): Boolean =
    schema.contains(ColumnPath(column.getColumnPath))

  override def visit[T <: Comparable[T]](eq: Operators.Eq[T]): FilterPredicate =
    if (isPartitionFilter(eq.getColumn)) eq
    else AssumeTrue

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
      case (AssumeTrue, right)      => right
      case (left, AssumeTrue)       => left
      case (left, right)            => FilterApi.and(left, right)
    }

  override def visit(or: Operators.Or): FilterPredicate =
    (or.getLeft.accept(this), or.getRight.accept(this)) match {
      case (AssumeTrue, AssumeTrue) => AssumeTrue
      case (AssumeTrue, _)          => AssumeTrue
      case (_, AssumeTrue)          => AssumeTrue
      case (left, right)            => FilterApi.or(left, right)
    }

  override def visit(not: Operators.Not): FilterPredicate =
    not.getPredicate.accept(this) match {
      case AssumeTrue => AssumeTrue
      case other      => FilterApi.not(other)
    }

  override def visit[T <: Comparable[T], U <: UserDefinedPredicate[T]](
      udp: Operators.UserDefined[T, U]
  ): FilterPredicate =
    if (isPartitionFilter(udp.getColumn)) udp
    else AssumeTrue

  override def visit[T <: Comparable[T], U <: UserDefinedPredicate[T]](
      udp: Operators.LogicalNotUserDefined[T, U]
  ): FilterPredicate =
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

  /** Rewrites given filter predicate so that it doesn't contain conditions that are already met by partition filter.
    * Result shall be used to filter Parquet files.
    * @param filterPredicate
    *   predicate to rewrite
    * @param partitionView
    *   partition data against which filter has to be rewritten
    * @return
    *   rewritten filter predicate
    */
  def rewrite(filterPredicate: FilterPredicate, partitionView: PartitionView): FilterPredicate =
    filterPredicate.accept(new FilterRewriter(partitionView))

}

private class FilterRewriter(partitionView: PartitionView) extends FilterPredicate.Visitor[FilterPredicate] {

  import FilterRewriter.*

  private def isPartitionFilter(column: Column[?]): Boolean =
    partitionView.schema.contains(ColumnPath(column.getColumnPath))

  private def evaluate(filterPredicate: FilterPredicate): FilterPredicate =
    if (filterPredicate.accept(new PartitionFilter(partitionView))) IsTrue
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
      case (IsFalse, _)     => IsFalse
      case (_, IsFalse)     => IsFalse
      case (IsTrue, IsTrue) => IsTrue
      case (IsTrue, right)  => right
      case (left, IsTrue)   => left
      case (left, right)    => FilterApi.and(left, right)
    }

  override def visit(or: Operators.Or): FilterPredicate =
    (or.getLeft.accept(this), or.getRight.accept(this)) match {
      case (IsTrue, _)        => IsTrue
      case (_, IsTrue)        => IsTrue
      case (IsFalse, IsFalse) => IsFalse
      case (IsFalse, right)   => right
      case (left, IsFalse)    => left
      case (left, right)      => FilterApi.or(left, right)
    }

  override def visit(not: Operators.Not): FilterPredicate =
    not.getPredicate.accept(this) match {
      case IsTrue    => IsFalse
      case IsFalse   => IsTrue
      case predicate => FilterApi.not(predicate)
    }

  override def visit[T <: Comparable[T], U <: UserDefinedPredicate[T]](
      udp: Operators.UserDefined[T, U]
  ): FilterPredicate =
    if (isPartitionFilter(udp.getColumn)) evaluate(udp)
    else udp

  override def visit[T <: Comparable[T], U <: UserDefinedPredicate[T]](
      udp: Operators.LogicalNotUserDefined[T, U]
  ): FilterPredicate =
    if (isPartitionFilter(udp.getUserDefined.getColumn)) evaluate(udp)
    else udp

}
