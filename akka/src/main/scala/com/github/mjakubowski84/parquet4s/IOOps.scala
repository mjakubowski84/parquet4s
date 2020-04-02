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

  private type Partition = (String, String)

  private val PartitionRegexp: Regex = """(\w+)=(\w+)""".r

}

trait IOOps {

  import IOOps._

  protected val logger: Logger

  protected def validateWritePath(path: Path,
                                  writeOptions: ParquetWriter.Options): Unit = {
    val fs = path.getFileSystem(writeOptions.hadoopConf)
    try {
      if (fs.exists(path)) {
        if (writeOptions.writeMode == ParquetFileWriter.Mode.CREATE)
          throw new AlreadyExistsException(
            s"File or directory already exists: $path"
          )
        else {
          if (logger.isDebugEnabled)
            logger.debug(s"Deleting $path in order to override with new data.")
          fs.delete(path, true)
        }
      }
    } finally fs.close()
  }

  protected def filesAtPath(path: Path, configuration: Configuration)(
    implicit ec: ExecutionContext
  ): Future[List[String]] = Future {
    scala.concurrent.blocking {
      val fs = path.getFileSystem(configuration)
      try {
        fs.listFiles(path, false)
          .map(_.getPath.getName)
          .toList
      } finally fs.close()
    }
  }

  protected def filesAtPath(path: String, configuration: Configuration)(
    implicit ec: ExecutionContext
  ): Future[List[String]] = filesAtPath(new Path(path), configuration)

  protected def findPartitionedPaths(path: Path,
                                     configuration: Configuration): Either[Exception, PartitionedDirectory] = {
    val fs = path.getFileSystem(configuration)
    try {
      PartitionedDirectory(findPartitionedPaths(fs, path, List.empty))
    } finally fs.close()
  }

  protected def findPartitionedPaths(
    path: String,
    configuration: Configuration
  ): Either[Exception, PartitionedDirectory] =
    findPartitionedPaths(new Path(path), configuration)

  private def findPartitionedPaths(
    fs: FileSystem,
    path: Path,
    partitions: List[Partition]
  ): List[PartitionedPath] = {
    val partitionedDirs = listDirs(fs, path).flatMap(matchPartition)
    if (partitionedDirs.isEmpty)
      List(PartitionedPath(path, partitions))
    else
      partitionedDirs.flatMap {
        case (subPath, partition) =>
          findPartitionedPaths(fs, subPath, partitions :+ partition)
      }
  }

  private def listDirs(fs: FileSystem, path: Path): List[FileStatus] =
    fs.listStatus(path).toList.filter(_.isDirectory)

  private def matchPartition(
    fileStatus: FileStatus
  ): Option[(Path, Partition)] = {
    val path = fileStatus.getPath
    path.getName match {
      case PartitionRegexp(name, value) => Some(path, (name, value))
      case _                            => None
    }
  }

}
