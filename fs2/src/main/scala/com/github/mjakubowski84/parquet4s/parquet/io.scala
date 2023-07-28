package com.github.mjakubowski84.parquet4s.parquet

import cats.effect.Sync
import com.github.mjakubowski84.parquet4s.{ColumnPath, ParquetWriter, PartitionedDirectory, PartitionedPath, Path}
import org.apache.hadoop.fs.{FileAlreadyExistsException, FileStatus, FileSystem}
import org.apache.parquet.hadoop.ParquetFileWriter
import cats.implicits.*
import com.github.mjakubowski84.parquet4s.parquet.logger.Logger
import org.apache.hadoop.conf.Configuration
import fs2.Stream
import org.apache.parquet.hadoop.util.HiddenFileFilter

import scala.util.matching.Regex

private[parquet] object io {

  private type Partition = (ColumnPath, String)

  sealed private trait StatusAccumulator
  private case object Empty extends StatusAccumulator
  private object Dirs {
    def apply(partitionPath: (Path, Partition)): Dirs = Dirs(Vector(partitionPath))
  }
  private case class Dirs(partitionPaths: Vector[(Path, Partition)]) extends StatusAccumulator
  private case class Files(files: Vector[FileStatus]) extends StatusAccumulator

  private[parquet4s] val PartitionRegexp: Regex = """([a-zA-Z0-9._]+)=([a-zA-Z0-9!?\-+_.,*'()&$@:;/ ]+)""".r

  def validateWritePath[F[_]](path: Path, writeOptions: ParquetWriter.Options, logger: Logger[F])(implicit
      F: Sync[F]
  ): F[Unit] = {
    val hadoopPath = path.toHadoop
    F.blocking(hadoopPath.getFileSystem(writeOptions.hadoopConf))
      .flatMap { fs =>
        F.blocking(fs.exists(hadoopPath)).flatMap {
          case true =>
            F.blocking(fs.getFileStatus(path.toHadoop)).map(_.isDirectory()).flatMap {
              case false if writeOptions.writeMode == ParquetFileWriter.Mode.CREATE =>
                F.raiseError(new FileAlreadyExistsException(s"File already exists: $path"))
              case false =>
                logger.debug(s"Deleting file $path in order to overwrite with new data.") >>
                  F.blocking(fs.delete(path.toHadoop, true)).void
              case true if writeOptions.writeMode == ParquetFileWriter.Mode.CREATE =>
                F.unit
              case true =>
                logger.debug(s"Deleting directory $path in order to overwrite with new data.") >>
                  F.blocking(fs.delete(path.toHadoop, true)).void
            }
          case false =>
            F.unit
        }
      }
  }

  def findPartitionedPaths[F[_]](path: Path, configuration: Configuration, logger: Logger[F])(implicit
      F: Sync[F]
  ): Stream[F, PartitionedDirectory] =
    Stream
      .eval(F.blocking(path.toHadoop.getFileSystem(configuration)))
      .flatMap(fs => findPartitionedPaths(fs, configuration, path, logger, List.empty))
      .fold[Either[Seq[Path], Seq[PartitionedPath]]](Right(Vector.empty)) {
        case (Left(invalidPaths), Left(moreInvalidPaths)) =>
          Left(invalidPaths ++ moreInvalidPaths)
        case (Right(partitionedPaths), Right(morePartitionedPaths)) =>
          Right(partitionedPaths ++ morePartitionedPaths)
        case (left: Left[?, ?], _) =>
          left
        case (_, left: Left[?, ?]) =>
          left
      }
      .map {
        case Left(invalidPaths)      => PartitionedDirectory.failed(invalidPaths)
        case Right(partitionedPaths) => PartitionedDirectory(partitionedPaths)
      }
      .flatMap(Stream.fromEither[F].apply)

  private def findPartitionedPaths[F[_]](
      fs: FileSystem,
      configuration: Configuration,
      path: Path,
      logger: Logger[F],
      partitions: List[Partition]
  )(implicit
      F: Sync[F]
  ): Stream[F, Either[Seq[Path], Seq[PartitionedPath]]] =
    Stream
      .evalSeq(F.blocking(fs.listStatus(path.toHadoop, HiddenFileFilter.INSTANCE).toVector))
      .fold[StatusAccumulator](Empty) {
        case (Empty, status) if status.isDirectory =>
          matchPartition(status).fold[StatusAccumulator](Empty)(Dirs.apply)
        case (Empty, status) =>
          Files(Vector(status))
        case (dirs @ Dirs(partitionPaths), status) if status.isDirectory =>
          matchPartition(status).fold(dirs)(partitionPath => Dirs(partitionPaths :+ partitionPath))
        case (_: Dirs, _) =>
          throw new RuntimeException("Inconsistent directory")
        case (_: Files, status) if status.isDirectory =>
          throw new RuntimeException("Inconsistent directory")
        case (Files(files), status) =>
          Files(files :+ status)
      }
      .flatMap {
        case Dirs(partitionPaths) => // node of directory tree
          Stream.emits(partitionPaths).flatMap { case (subPath, partition) =>
            findPartitionedPaths(fs, configuration, subPath, logger, partitions :+ partition)
          }
        case Files(files) => // leaf of directory tree
          Stream.emit(Right(files.map(fileStatus => PartitionedPath(fileStatus, configuration, partitions))))
        case Empty => // avoid redundant scans of empty dirs
          Stream.empty
      }
      .handleErrorWith(err =>
        Stream.eval(logger.debug(s"Error while fetching partitions: ${err.getMessage}")) >> Stream.emit(
          Left(Vector(path))
        )
      ) // mixture of dirs and files

  private def matchPartition(fileStatus: FileStatus): Option[(Path, Partition)] = {
    val path = Path(fileStatus.getPath)
    path.name match {
      case PartitionRegexp(name, value) => Some(path, (ColumnPath(name), value))
      case _                            => None
    }
  }

}
