package com.github.mjakubowski84.parquet4s.parquet

import cats.effect.Sync
import com.github.mjakubowski84.parquet4s.*
import org.apache.hadoop.fs.{FileAlreadyExistsException, FileStatus, FileSystem}
import org.apache.parquet.filter2.predicate.FilterPredicate
import org.apache.parquet.hadoop.ParquetFileWriter
import cats.implicits.*
import com.github.mjakubowski84.parquet4s.PartitionedPath.Partition
import com.github.mjakubowski84.parquet4s.parquet.logger.Logger
import org.apache.hadoop.conf.Configuration
import fs2.Stream
import org.apache.parquet.hadoop.util.HiddenFileFilter

import java.net.URLDecoder
import java.nio.charset.StandardCharsets
import scala.util.matching.Regex

private[parquet] object io {

  sealed private trait StatusAccumulator
  private case object Empty extends StatusAccumulator
  private object Dirs {
    def apply(partitionPath: (Path, Partition)): Dirs = Dirs(Vector(partitionPath))
  }
  private case class Dirs(partitionPaths: Vector[(Path, Partition)]) extends StatusAccumulator
  private case class Files(files: Vector[FileStatus]) extends StatusAccumulator

  private[parquet4s] val PartitionRegexp: Regex = IOOps.PartitionRegexp

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

  @deprecated(message = "Use listPartitionedDirectory instead", since = "2.17.0")
  def findPartitionedPaths[F[_]](
      path: Path,
      configuration: Configuration,
      logger: Logger[F]
  )(implicit F: Sync[F]): Stream[F, PartitionedDirectory] =
    listPartitionedDirectory(path, configuration, logger, Filter.noopFilter, ValueCodecConfiguration.Default)

  /** Traverses a tree of files which may cointain Hive-style partitions. Applies a provider filter to paths to
    * eliminate redundant list operations on unmatched directories. The resulting [[PartitionedDirectory]] contains a
    * list of all matching paths accompanied with a rewritten, simplified filter so that file filter does not contain
    * references to partition fields.
    *
    * @param path
    *   directory path
    * @param configuration
    *   Hadoop's [[org.apache.hadoop.conf.Configuration]]
    * @param logger
    *   logger
    * @param filter
    *   filter to be applied to partitions and files
    * @param valueCodecConfiguration
    *   required to decode filter value
    * @param F
    *   effect
    * @return
    *   [[fs2.Stream]] of single [[PartitionedDirectory]]
    */
  def listPartitionedDirectory[F[_]](
      path: Path,
      configuration: Configuration,
      logger: Logger[F],
      filter: Filter,
      valueCodecConfiguration: ValueCodecConfiguration
  )(implicit F: Sync[F]): Stream[F, PartitionedDirectory] = {
    val filterPredicateOptStream = filter match {
      case _: RecordFilter | Filter.noopFilter => Stream.emit(None)
      case filter => Stream.eval(F.catchNonFatal(Option(filter.toPredicate(valueCodecConfiguration))))
    }
    val partitionedPathsStream = for {
      filterPredicateOpt <- filterPredicateOptStream
      fs                 <- Stream.eval(F.blocking(path.toHadoop.getFileSystem(configuration)))
      partitionedPaths   <- listPartitionedDirectory(fs, configuration, path, logger, filterPredicateOpt, List.empty)
    } yield partitionedPaths

    partitionedPathsStream
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
  }

  private def listPartitionedDirectory[F[_]](
      fs: FileSystem,
      configuration: Configuration,
      path: Path,
      logger: Logger[F],
      filterPredicateOpt: Option[FilterPredicate],
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
          val partitionedDirs = filterPredicateOpt match {
            case Some(filterPredicate) =>
              // filters subPaths early
              Stream.evalSeq(
                F.catchNonFatal(PartitionFilter.filterPartitionPaths(filterPredicate, partitions, partitionPaths))
              )
            case None =>
              Stream.iterable(partitionPaths).map { case (subPath, partition) => subPath -> (partitions :+ partition) }
          }
          partitionedDirs.flatMap { case (subPath, partitions) =>
            listPartitionedDirectory(fs, configuration, subPath, logger, filterPredicateOpt, partitions)
          }
        case Files(files) => // leaf of directory tree
          filterPredicateOpt match {
            case None =>
              Stream.emit(Right(files.map(fileStatus => PartitionedPath(fileStatus, configuration, partitions, None))))
            case Some(filterPredicate) =>
              Stream
                .eval(F.catchNonFatal(FilterRewriter.rewrite(filterPredicate, PartitionView(partitions))))
                .evalMapChunk[F, Either[Seq[Path], Seq[PartitionedPath]]] {
                  case FilterRewriter.IsTrue =>
                    logger.debug(
                      s"Dropping filter at path $path as partition exhausts filter predicate $filterPredicate."
                    ) >>
                      F.delay(
                        Right(files.map(fileStatus => PartitionedPath(fileStatus, configuration, partitions, None)))
                      )
                  case FilterRewriter.IsFalse =>
                    logger.debug(
                      s"Skipping files at path $path as partition does not match filter predicate $filterPredicate."
                    ) >>
                      F.pure(Right(List.empty))
                  case rewritten =>
                    logger.debug(
                      s"Filter predicate $filterPredicate for path $path has been rewritten to $rewritten."
                    ) >>
                      F.delay(
                        Right(
                          files.map(fileStatus =>
                            PartitionedPath(fileStatus, configuration, partitions, Option(rewritten))
                          )
                        )
                      )
                }
          }
        case Empty =>
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
      case PartitionRegexp(name, value) =>
        Some(path -> (ColumnPath(name) -> URLDecoder.decode(value, StandardCharsets.UTF_8.name())))
      case _ => None
    }
  }

}
