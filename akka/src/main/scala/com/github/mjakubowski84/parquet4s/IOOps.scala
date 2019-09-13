package com.github.mjakubowski84.parquet4s

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.SecureIOUtils.AlreadyExistsException
import org.apache.parquet.hadoop.ParquetFileWriter
import org.slf4j.Logger

import scala.concurrent.{ExecutionContext, Future}
import scala.util.Try

trait IOOps {

  protected val logger: Logger

  protected def validateWritePath(path: Path, writeOptions: ParquetWriter.Options): Unit = {
    val fs = path.getFileSystem(writeOptions.hadoopConf)
    try {
      if (fs.exists(path)) {
        if (writeOptions.writeMode == ParquetFileWriter.Mode.CREATE)
          throw new AlreadyExistsException(s"File or directory already exists: $path")
        else {
          if (logger.isDebugEnabled) logger.debug(s"Deleting $path in order to override with new data.")
          fs.delete(path, true)
        }
      }
    } finally fs.close()
  }

  protected def filesAtPath(path: Path, writeOptions: ParquetWriter.Options)
                           (implicit ec: ExecutionContext): Future[List[String]] = Future {
    scala.concurrent.blocking {
      val fs = path.getFileSystem(writeOptions.hadoopConf)
      try {
        val iter = fs.listFiles(path, false)
        Stream
          .continually(Try(iter.next()))
          .takeWhile(_.isSuccess)
          .map(_.get)
          .map(_.getPath.getName)
          .toList
      } finally fs.close()
    }
  }

  protected def filesAtPath(path: String, writeOptions: ParquetWriter.Options)
                           (implicit ec: ExecutionContext): Future[List[String]] = filesAtPath(new Path(path), writeOptions)

}
