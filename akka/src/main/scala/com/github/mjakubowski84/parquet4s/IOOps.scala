package com.github.mjakubowski84.parquet4s

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileStatus, FileSystem, Path, RemoteIterator}
import org.apache.hadoop.io.SecureIOUtils.AlreadyExistsException
import org.apache.parquet.hadoop.ParquetFileWriter
import org.slf4j.Logger

import scala.concurrent.{ExecutionContext, Future}
import scala.language.implicitConversions
import scala.util.matching.Regex

private[parquet4s] object IOOps {

  private implicit class RemoteIteratorWrapper[T](wrapped: RemoteIterator[T]) extends Iterator[T] {
    override def hasNext: Boolean = wrapped.hasNext
    override def next(): T = wrapped.next()
  }

  private type Partition = (ColumnPath, String)

  private[parquet4s] val PartitionRegexp: Regex = """([a-zA-Z0-9._]+)=([a-zA-Z0-9!\-_.*'()]+)""".r

}

/**
 * Utility functions that perform operations on HDFS.
 */
trait IOOps {

  import IOOps._

  protected val logger: Logger

  protected def validateWritePath(path: Path,
                                  writeOptions: ParquetWriter.Options): Unit = {
    val fs = path.getFileSystem(writeOptions.hadoopConf)
    try {
      if (fs.exists(path)) {
        if (writeOptions.writeMode == ParquetFileWriter.Mode.CREATE)
          throw new AlreadyExistsException(s"File or directory already exists: $path")
        else {
          if (logger.isDebugEnabled)
            logger.debug(s"Deleting $path in order to override with new data.")
          fs.delete(path, true)
        }
      }
    } finally fs.close()
  }

  protected def filesAtPath(path: Path, configuration: Configuration)
                           (implicit ec: ExecutionContext): Future[List[String]] =
    Future {
      scala.concurrent.blocking {
        val fs = path.getFileSystem(configuration)
        try {
          fs.listFiles(path, false)
            .map(_.getPath.getName)
            .toList
        } finally fs.close()
      }
    }

  protected def filesAtPath(path: String, configuration: Configuration)
                           (implicit ec: ExecutionContext): Future[List[String]] =
    filesAtPath(new Path(path), configuration)

  protected def findPartitionedPaths(path: Path,
                                     configuration: Configuration): Either[Exception, PartitionedDirectory] = {
    val fs = path.getFileSystem(configuration)
    try {
      findPartitionedPaths(fs, path, List.empty).fold(
        PartitionedDirectory.failed,
        PartitionedDirectory.apply
      )
    } finally fs.close()
  }

  protected def findPartitionedPaths(path: String,
                                     configuration: Configuration): Either[Exception, PartitionedDirectory] =
    findPartitionedPaths(new Path(path), configuration)

  private def findPartitionedPaths(fs: FileSystem,
                                   path: Path,
                                   partitions: List[Partition]): Either[List[Path], List[PartitionedPath]] = {
    val (dirs, files) = fs.listStatus(path).toList.partition(_.isDirectory)
    if (dirs.nonEmpty && files.nonEmpty)
      Left(path :: Nil) // path is invalid because it contains both dirs and files
    else { // TODO do not return leaf nodes that are empty
      val partitionedDirs = dirs.flatMap(matchPartition)
      if (partitionedDirs.isEmpty)
        Right(List(PartitionedPath(path, partitions))) // leaf dir
      else
        partitionedDirs
          .map { case (subPath, partition) => findPartitionedPaths(fs, subPath, partitions :+ partition) }
          .foldLeft[Either[List[Path], List[PartitionedPath]]](Right(List.empty)) {
            case (Left(invalidPaths), Left(moreInvalidPaths)) =>
              Left(invalidPaths ++ moreInvalidPaths)
            case (Right(partitionedPaths), Right(morePartitionedPaths)) =>
              Right(partitionedPaths ++ morePartitionedPaths)
            case (left: Left[_, _], _) =>
              left
            case (_, left: Left[_, _]) =>
              left
        }
    }
  }

  private def matchPartition(fileStatus: FileStatus): Option[(Path, Partition)] = {
    val path = fileStatus.getPath
    path.getName match {
      case PartitionRegexp(name, value) => Some(path, (ColumnPath(name), value))
      case _                            => None
    }
  }

}
