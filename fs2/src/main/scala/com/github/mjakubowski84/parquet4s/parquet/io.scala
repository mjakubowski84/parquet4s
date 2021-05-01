package com.github.mjakubowski84.parquet4s.parquet

import cats.effect.{Resource, Sync}
import com.github.mjakubowski84.parquet4s.{ParquetWriter, PartitionedDirectory, PartitionedPath}
import org.apache.hadoop.fs.{FileStatus, FileSystem, Path}
import org.apache.hadoop.io.SecureIOUtils.AlreadyExistsException
import org.apache.parquet.hadoop.ParquetFileWriter
import cats.implicits._
import com.github.mjakubowski84.parquet4s.parquet.logger.Logger
import org.apache.hadoop.conf.Configuration
import fs2.Stream

import scala.language.higherKinds
import scala.util.matching.Regex

private[parquet] object io {

  private type Partition = (String, String)

  private trait StatusAccumulator
  private case object Empty extends StatusAccumulator
  private object Dirs {
    def apply(partitionPath: (Path, Partition)): Dirs = Dirs(Vector(partitionPath))
  }
  private case class Dirs(partitionPaths: Vector[(Path, Partition)]) extends StatusAccumulator
  private case object Files extends StatusAccumulator

  private[parquet4s] val PartitionRegexp: Regex = """([a-zA-Z0-9._]+)=([a-zA-Z0-9!\-_.*'()]+)""".r

  def makePath[F[_]](path: String)(implicit F: Sync[F]): F[Path] = F.catchNonFatal(new Path(path))

  def validateWritePath[F[_]](path: Path,
                              writeOptions: ParquetWriter.Options,
                              logger: Logger[F]
                             )(implicit F: Sync[F]): F[Unit] = {
    Resource
      .fromAutoCloseable(F.blocking(path.getFileSystem(writeOptions.hadoopConf)))
      .use { fs =>
        F.blocking(fs.exists(path)).flatMap {
          case true if writeOptions.writeMode == ParquetFileWriter.Mode.CREATE =>
            F.raiseError(new AlreadyExistsException(s"File or directory already exists: $path"))
          case true =>
            logger.debug(s"Deleting $path in order to override with new data.") >>
              F.blocking(fs.delete(path, true)).void
          case false =>
            F.unit
        }
      }
  }

  def findPartitionedPaths[F[_]](path: Path,
                                 configuration: Configuration)
                                (implicit F: Sync[F]): Stream[F, PartitionedDirectory] =
    Stream.resource(Resource.fromAutoCloseable(F.blocking(path.getFileSystem(configuration))))
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
        case Left(invalidPaths) => PartitionedDirectory.failed(invalidPaths)
        case Right(partitionedPaths) => PartitionedDirectory(partitionedPaths)
      }
      .flatMap(Stream.fromEither[F].apply)

  private def findPartitionedPaths[F[_]](fs: FileSystem,
                                         path: Path,
                                         partitions: List[Partition]
                                        )(implicit F: Sync[F]): Stream[F, Either[Seq[Path], Seq[PartitionedPath]]] =
    Stream.evalSeq(F.blocking(fs.listStatus(path).toVector))
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
        case Dirs(partitionPaths) => // node od directory tree
          Stream.emits(partitionPaths).flatMap {
            case (subPath, partition) => findPartitionedPaths(fs, subPath, partitions :+ partition)
          }
        case Files => // leaf of directory tree
          Stream.emit(Right(Vector(PartitionedPath(path, partitions))))
        case Empty => // avoid redundant scans of empty dirs
          Stream.empty
      }
      .handleErrorWith(_ => Stream.emit(Left(Vector(path)))) // mixture of dirs and files

  private def matchPartition(fileStatus: FileStatus): Option[(Path, Partition)] = {
    val path = fileStatus.getPath
    path.getName match {
      case PartitionRegexp(name, value) => Some(path, (name, value))
      case _                            => None
    }
  }

}
