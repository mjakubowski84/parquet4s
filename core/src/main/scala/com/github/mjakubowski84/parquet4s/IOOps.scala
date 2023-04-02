package com.github.mjakubowski84.parquet4s

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileAlreadyExistsException, FileStatus, FileSystem, RemoteIterator}
import org.apache.parquet.filter2.predicate.FilterPredicate
import org.apache.parquet.hadoop.ParquetFileWriter
import org.apache.parquet.hadoop.util.HiddenFileFilter
import org.slf4j.Logger

import scala.concurrent.{ExecutionContext, Future}
import scala.language.implicitConversions
import scala.util.matching.Regex

private[parquet4s] object IOOps {

  implicit private class RemoteIteratorWrapper[T](wrapped: RemoteIterator[T]) extends Iterator[T] {
    override def hasNext: Boolean = wrapped.hasNext
    override def next(): T        = wrapped.next()
  }

  private type Partition = (ColumnPath, String)

  private[parquet4s] val PartitionRegexp: Regex = """([a-zA-Z0-9._]+)=([a-zA-Z0-9!?\-+_.,*'()&$@:;/ ]+)""".r

}

/** Utility functions that perform operations on HDFS.
  */
trait IOOps {

  import IOOps.*

  protected val logger: Logger

  protected def validateWritePath(path: Path, writeOptions: ParquetWriter.Options): Unit = {
    val hadoopPath = path.toHadoop
    val fs         = hadoopPath.getFileSystem(writeOptions.hadoopConf)
    if (fs.exists(hadoopPath)) {
      fs.getFileStatus(path.toHadoop).isDirectory match {
        case false if writeOptions.writeMode == ParquetFileWriter.Mode.CREATE =>
          throw new FileAlreadyExistsException(s"File or directory already exists: $hadoopPath")
        case false =>
          if (logger.isDebugEnabled)
            logger.debug(s"Deleting file $hadoopPath in order to override with new data.")
          fs.delete(hadoopPath, true)
          ()
        case true if writeOptions.writeMode == ParquetFileWriter.Mode.CREATE =>
          ()
        case true =>
          if (logger.isDebugEnabled)
            logger.debug(s"Deleting directory $hadoopPath in order to override with new data.")
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

  protected def findPartitionedPaths(
      path: Path,
      configuration: Configuration,
      filterPredicate: FilterPredicate
  ): Either[Exception, PartitionedDirectory] = {
    val fs = path.toHadoop.getFileSystem(configuration)
    findPartitionedPaths(fs, configuration, path, List.empty, filterPredicate).fold(
      PartitionedDirectory.failed,
      PartitionedDirectory.apply
    )
  }

  private def findPartitionedPaths(
      fs: FileSystem,
      configuration: Configuration,
      path: Path,
      partitions: List[Partition],
      filterPredicate: FilterPredicate
  ): Either[List[Path], List[PartitionedPath]] = {
    val (dirs, files) = fs
      .listStatus(path.toHadoop, HiddenFileFilter.INSTANCE)
      .toList
      .partition(_.isDirectory)
    if (dirs.nonEmpty && files.nonEmpty)
      Left(path :: Nil) // path is invalid because it contains both dirs and files
    else {
      val partitionedDirs: Seq[(Path, (ColumnPath, String))] = dirs.flatMap(matchPartition)
      if (partitionedDirs.isEmpty && files.isEmpty)
        Right(List.empty) // empty dir
      else if (partitionedDirs.isEmpty) {
        // leaf file
        Right(files.map(fileStatus => PartitionedPath(fileStatus, configuration, partitions)))
      } else {
        // node dir with files
        PartitionFilter
          .filterPartial(
            filterPredicate  = filterPredicate,
            commonPartitions = partitions,
            partitionedDirs  = partitionedDirs
          )
          .map { case (subPath, partitions) =>
            findPartitionedPaths(fs, configuration, subPath, partitions, filterPredicate)
          }
          .foldLeft[Either[List[Path], List[PartitionedPath]]](Right(List.empty)) {
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
      case PartitionRegexp(name, value) => Some(path, (ColumnPath(name), value))
      case _                            => None
    }
  }

}
