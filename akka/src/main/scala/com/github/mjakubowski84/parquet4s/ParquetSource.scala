package com.github.mjakubowski84.parquet4s

import akka.NotUsed
import akka.stream.scaladsl.Source
import com.github.mjakubowski84.parquet4s.IOOps.PartitionedPath
import org.apache.hadoop.fs.Path
import org.apache.hadoop.ipc.protobuf.RpcHeaderProtos.RpcSaslProto.SaslAuthOrBuilder
import org.apache.parquet.filter2.predicate.{FilterPredicate, Operators, UserDefinedPredicate}
import org.apache.parquet.hadoop.{ParquetReader => HadoopParquetReader}
import org.apache.parquet.io.api.Binary
import org.slf4j.{Logger, LoggerFactory}

import scala.util.Try

private[parquet4s] object ParquetSource extends IOOps {

  override protected lazy val logger: Logger = LoggerFactory.getLogger(this.getClass)

  def apply[T : ParquetRecordDecoder](path: Path,
                                      options: ParquetReader.Options,
                                      filter: Filter
                                     ): Source[T, NotUsed] = {
    val valueCodecConfiguration = options.toValueCodecConfiguration
    val hadoopConf = options.hadoopConf

    findPartitionedPaths(path, hadoopConf).fold(
      Source.failed,
      partitionedPaths => {
        val filteredPartitionPaths =
          if (filter == Filter.noopFilter) partitionedPaths
          else partitionedPaths.filter(PartitionFilter.filterBy(filter.toPredicate(valueCodecConfiguration)))

        filteredPartitionPaths.foldLeft(Source.empty[T]) { case (previousSource, partitionedPath) =>

          val builder = HadoopParquetReader.builder[RowParquetRecord](new ParquetReadSupport(), partitionedPath.path)
            .withConf(hadoopConf)
            .withFilter(filter.toFilterCompat(valueCodecConfiguration))

          def decode(record: RowParquetRecord): T = ParquetRecordDecoder.decode[T](record, valueCodecConfiguration)

          val newSource = Source.unfoldResource[RowParquetRecord, HadoopParquetReader[RowParquetRecord]](
            create = builder.build,
            read = reader => Option(reader.read()),
            close = _.close()
          ).map { record =>
            partitionedPath.partitionMap.foreach { case (name, value) =>
              record.add(name, BinaryValue(Binary.fromString(value)))
            }
            record
          }.map(decode)

          previousSource.concat(newSource)
        }
      }
    )
  }

  object PartitionFilter {

    def filterBy(filterPredicate: FilterPredicate)(partitionedPath: PartitionedPath): Boolean =
      filterPredicate.accept(new PartitionFilter(partitionedPath))

  }

  class PartitionFilter(partitionedPath: PartitionedPath) extends FilterPredicate.Visitor[Boolean] {

    // TODO partitionPath can already contain Binary Values

    override def visit[T <: Comparable[T]](eq: Operators.Eq[T]): Boolean = {
      val columnPath = eq.getColumn.getColumnPath.toString

      eq.getValue match {
        case binary: Binary => ???
        case other => throw new IllegalArgumentException("filter type does not match schema, column <name> is such and while filter is such") // give name of column, g
      }

      partitionedPath.partitionMap.get(columnPath) match {
        case None => true
        case Some(partitionValue) => eq.getValue.toString == partitionValue
      }
    }

    override def visit[T <: Comparable[T]](notEq: Operators.NotEq[T]): Boolean = {
      val columnPath = notEq.getColumn.getColumnPath.toString
      partitionedPath.partitionMap.get(columnPath) match {
        case None => true
        case Some(partitionValue) => notEq.getValue.toString != partitionValue
      }
    }

    override def visit[T <: Comparable[T]](lt: Operators.Lt[T]): Boolean = ???

    override def visit[T <: Comparable[T]](ltEq: Operators.LtEq[T]): Boolean = ???

    override def visit[T <: Comparable[T]](gt: Operators.Gt[T]): Boolean = ???

    override def visit[T <: Comparable[T]](gtEq: Operators.GtEq[T]): Boolean = ???

    override def visit(and: Operators.And): Boolean =
      and.getLeft.accept(this) || and.getRight.accept(this)

    override def visit(or: Operators.Or): Boolean =
      or.getLeft.accept(this) && or.getRight.accept(this)

    override def visit(not: Operators.Not): Boolean = !not.accept(this)

    override def visit[T <: Comparable[T], U <: UserDefinedPredicate[T]](udp: Operators.UserDefined[T, U]): Boolean = ???

    override def visit[T <: Comparable[T], U <: UserDefinedPredicate[T]](udp: Operators.LogicalNotUserDefined[T, U]): Boolean = ???
  }

}
