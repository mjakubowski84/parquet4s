package com.github.mjakubowski84.parquet4s

import com.github.mjakubowski84.parquet4s.PartitionedPath.Partition
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileAlreadyExistsException, FileStatus, FileSystem, RemoteIterator}
import org.apache.parquet.filter2.predicate.FilterPredicate
import org.apache.parquet.hadoop.ParquetFileWriter
import org.apache.parquet.hadoop.util.HiddenFileFilter
import org.slf4j.Logger

import scala.concurrent.{ExecutionContext, Future}
import scala.util.matching.Regex
import java.net.URLDecoder
import java.nio.charset.StandardCharsets

private[parquet4s] object IOOps {

  implicit private class RemoteIteratorWrapper[T](wrapped: RemoteIterator[T]) extends Iterator[T] {
    override def hasNext: Boolean = wrapped.hasNext
    override def next(): T        = wrapped.next()
  }

  private[parquet4s] val PartitionRegexp: Regex = """([a-zA-Z0-9._]+)=([a-zA-Z0-9!?\-+_.,*'()&$@:;/ %]+)""".r

}

/** Utility functions that perform operations on HDFS.
  */
trait IOOps {

  import IOOps.*

  protected val logger: Logger

  private def debug(message: => String): Unit = if (logger.isDebugEnabled) logger.debug(message)

  protected def validateWritePath(path: Path, writeOptions: ParquetWriter.Options): Unit = {
    val hadoopPath = path.toHadoop
    val fs         = hadoopPath.getFileSystem(writeOptions.hadoopConf)
    if (fs.exists(hadoopPath)) {
      fs.getFileStatus(path.toHadoop).isDirectory match {
        case false if writeOptions.writeMode == ParquetFileWriter.Mode.CREATE =>
          throw new FileAlreadyExistsException(s"File or directory already exists: $hadoopPath")
        case false =>
          debug(s"Deleting file $hadoopPath in order to override with new data.")
          fs.delete(hadoopPath, true)
          ()
        case true if writeOptions.writeMode == ParquetFileWriter.Mode.CREATE =>
          ()
        case true =>
          debug(s"Deleting directory $hadoopPath in order to override with new data.")
          fs.delete(hadoopPath, true)
          ()
      }
    }
  }

  protected def filesAtPath(path: Path, configuration: Configuration)(implicit
      ec: ExecutionContext
  ): Future[List[String]] =
    Future {
      val hadoopPath = path.toHadoop
      scala.concurrent.blocking {
        hadoopPath
          .getFileSystem(configuration)
          .listFiles(hadoopPath, false)
          .map(_.getPath.getName)
          .toList
      }
    }

  @deprecated(message = "Use listPartitionedDirectory instead", since = "2.17.0")
  protected def findPartitionedPaths(
      path: Path,
      configuration: Configuration
  ): Either[Exception, PartitionedDirectory] =
    listPartitionedDirectory(path, configuration, Filter.noopFilter, ValueCodecConfiguration.Default)

  /** Traverses a tree of files which may cointain Hive-style partitions. Applies a provider filter to paths to
    * eliminate redundant list operations on unmatched directories. The resulting [[PartitionedDirectory]] contains a
    * list of all matching paths accompanied with a rewritten, simplified filter so that file filter does not contain
    * references to partition fields.
    *
    * @param path
    *   directory path
    * @param configuration
    *   Hadoop's [[org.apache.hadoop.conf.Configuration]]
    * @param filter
    *   filter to be applied to partitions and files
    * @param valueCodecConfiguration
    *   required to decode filter values
    * @return
    *   Exception or [[PartitionedDirectory]]
    */
  protected def listPartitionedDirectory(
      path: Path,
      configuration: Configuration,
      filter: Filter,
      valueCodecConfiguration: ValueCodecConfiguration
  ): Either[Exception, PartitionedDirectory] = {
    val filterPredicateOpt = filter match {
      case _: RecordFilter | Filter.noopFilter => None
      case filter                              => Option(filter.toPredicate(valueCodecConfiguration))
    }
    val fs = path.toHadoop.getFileSystem(configuration)
    listPartitionedDirectory(fs, configuration, path, List.empty, filterPredicateOpt).fold(
      PartitionedDirectory.failed,
      PartitionedDirectory.apply
    )
  }

  private def listPartitionedDirectory(
      fs: FileSystem,
      configuration: Configuration,
      path: Path,
      partitions: List[Partition],
      filterPredicateOpt: Option[FilterPredicate]
  ): Either[Vector[Path], Vector[PartitionedPath]] = {
    val (dirs, files) = fs
      .listStatus(path.toHadoop, HiddenFileFilter.INSTANCE)
      .toVector
      .partition(_.isDirectory)
    if (dirs.nonEmpty && files.nonEmpty)
      Left(Vector(path)) // path is invalid because it contains both dirs and files
    else {
      val partitionedDirs = dirs.flatMap(matchPartition)
      if (partitionedDirs.isEmpty && files.isEmpty)
        Right(Vector.empty) // empty dir
      else if (partitionedDirs.isEmpty) {
        // leaf files
        filterPredicateOpt match {
          case None =>
            Right(files.map(fileStatus => PartitionedPath(fileStatus, configuration, partitions, filterPredicateOpt)))
          case Some(filterPredicate) =>
            FilterRewriter.rewrite(filterPredicate, PartitionView(partitions)) match {
              case FilterRewriter.IsTrue =>
                debug(s"Dropping filter at path $path as partition exhausts filter predicate $filterPredicate.")
                Right(files.map(fileStatus => PartitionedPath(fileStatus, configuration, partitions, None)))
              case FilterRewriter.IsFalse =>
                debug(s"Skipping files at path $path as partition does not match filter predicate $filterPredicate.")
                Right(Vector.empty)
              case rewritten =>
                debug(s"Filter predicate $filterPredicate for path $path has been rewritten to $rewritten.")
                Right(
                  files.map(fileStatus => PartitionedPath(fileStatus, configuration, partitions, Option(rewritten)))
                )
            }
        }
      } else {
        // non-empty node dir
        // early removes paths which do not match filter predicate
        filterPredicateOpt
          .fold(partitionedDirs.map { case (path, partition) => path -> (partitions :+ partition) }) { filterPredicate =>
            PartitionFilter.filterPartitionPaths(
              filterPredicate  = filterPredicate,
              commonPartitions = partitions,
              partitionedPaths = partitionedDirs
            )
          }
          .map { case (subPath, partitions) =>
            listPartitionedDirectory(fs, configuration, subPath, partitions, filterPredicateOpt)
          }
          .foldLeft[Either[Vector[Path], Vector[PartitionedPath]]](Right(Vector.empty)) {
            case (Left(invalidPaths), Left(moreInvalidPaths)) =>
              Left(invalidPaths ++ moreInvalidPaths)
            case (Right(partitionedPaths), Right(morePartitionedPaths)) =>
              Right(partitionedPaths ++ morePartitionedPaths)
            case (left: Left[?, ?], _) =>
              left
            case (_, left: Left[?, ?]) =>
              left
          }
      }
    }
  }

  private def matchPartition(fileStatus: FileStatus): Option[(Path, Partition)] = {
    val path = Path(fileStatus.getPath)
    path.name match {
      case PartitionRegexp(name, value) =>
        Some(path -> (ColumnPath(name) -> URLDecoder.decode(value, StandardCharsets.UTF_8.name())))
      case _ => None
    }
  }

}
