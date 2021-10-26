package com.github.mjakubowski84.parquet4s.parquet

import cats.effect.{Resource, Sync}
import com.github.mjakubowski84.parquet4s.{ColumnPath, ParquetWriter, PartitionedDirectory, PartitionedPath, Path}
import org.apache.hadoop.fs.{FileStatus, FileSystem}
import org.apache.hadoop.io.SecureIOUtils.AlreadyExistsException
import org.apache.parquet.hadoop.ParquetFileWriter
import cats.implicits._
import com.github.mjakubowski84.parquet4s.parquet.logger.Logger
import org.apache.hadoop.conf.Configuration
import fs2.Stream

import scala.language.higherKinds
import scala.util.matching.Regex

private[parquet] object io {

  private type Partition = (ColumnPath, String)

  sealed private trait StatusAccumulator
  private case object Empty extends StatusAccumulator
  private object Dirs {
    def apply(partitionPath: (Path, Partition)): Dirs = Dirs(Vector(partitionPath))
  }
  private case class Dirs(partitionPaths: Vector[(Path, Partition)]) extends StatusAccumulator
  private case object Files extends StatusAccumulator

  private[parquet4s] val PartitionRegexp: Regex = """([a-zA-Z0-9._]+)=([a-zA-Z0-9!\-_.*'()]+)""".r

  def validateWritePath[F[_]](path: Path, writeOptions: ParquetWriter.Options, logger: Logger[F])(implicit
      F: Sync[F]
  ): F[Unit] =
    Resource
      .fromAutoCloseable(F.blocking(path.toHadoop.getFileSystem(writeOptions.hadoopConf)))
      .use { fs =>
        F.blocking(fs.exists(path.toHadoop)).flatMap {
          case true if writeOptions.writeMode == ParquetFileWriter.Mode.CREATE =>
            F.raiseError(new AlreadyExistsException(s"File or directory already exists: $path"))
          case true =>
            logger.debug(s"Deleting $path in order to override with new data.") >>
              F.blocking(fs.delete(path.toHadoop, true)).void
          case false =>
            F.unit
        }
      }

  def findPartitionedPaths[F[_]](path: Path, configuration: Configuration)(implicit
      F: Sync[F]
  ): Stream[F, PartitionedDirectory] =
    Stream
      .resource(Resource.fromAutoCloseable(F.blocking(path.toHadoop.getFileSystem(configuration))))
      .flatMap(fs => findPartitionedPaths(fs, path, List.empty))
      .fold[Either[Seq[Path], Seq[PartitionedPath]]](Right(Vector.empty)) {
        case (Left(invalidPaths), Left(moreInvalidPaths)) =>
          Left(invalidPaths ++ moreInvalidPaths)
        case (Right(partitionedPaths), Right(morePartitionedPaths)) =>
          Right(partitionedPaths ++ morePartitionedPaths)
        case (left: Left[_, _], _) =>
          left
        case (_, left: Left[_, _]) =>
          left
      }
      .map {
        case Left(invalidPaths)      => PartitionedDirectory.failed(invalidPaths)
        case Right(partitionedPaths) => PartitionedDirectory(partitionedPaths)
      }
      .flatMap(Stream.fromEither[F].apply)

  private def findPartitionedPaths[F[_]](fs: FileSystem, path: Path, partitions: List[Partition])(implicit
      F: Sync[F]
  ): Stream[F, Either[Seq[Path], Seq[PartitionedPath]]] =
    Stream
      .evalSeq(F.blocking(fs.listStatus(path.toHadoop).toVector))
      .fold[StatusAccumulator](Empty) {
        case (Empty, status) if status.isDirectory =>
          matchPartition(status).fold[StatusAccumulator](Empty)(Dirs.apply)
        case (Empty, _) =>
          Files
        case (dirs @ Dirs(partitionPaths), status) if status.isDirectory =>
          matchPartition(status).fold(dirs)(partitionPath => Dirs(partitionPaths :+ partitionPath))
        case (_: Dirs, _) =>
          throw new RuntimeException("Inconsistent directory")
        case (Files, status) if status.isDirectory =>
          throw new RuntimeException("Inconsistent directory")
        case (Files, _) =>
          Files
      }
      .flatMap {
        case Dirs(partitionPaths) => // node of directory tree
          Stream.emits(partitionPaths).flatMap { case (subPath, partition) =>
            findPartitionedPaths(fs, subPath, partitions :+ partition)
          }
        case Files => // leaf of directory tree
          Stream.emit(Right(Vector(PartitionedPath(path, partitions))))
        case Empty => // avoid redundant scans of empty dirs
          Stream.empty
      }
      .handleErrorWith(_ => Stream.emit(Left(Vector(path)))) // mixture of dirs and files

  private def matchPartition(fileStatus: FileStatus): Option[(Path, Partition)] = {
    val path = Path(fileStatus.getPath)
    path.name match {
      case PartitionRegexp(name, value) => Some(path, (ColumnPath(name), value))
      case _                            => None
    }
  }

}
