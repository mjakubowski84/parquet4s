package com.github.mjakubowski84.parquet4s.parquet

import cats.effect.{Blocker, ContextShift, Resource, Sync}
import cats.implicits._
import com.github.mjakubowski84.parquet4s._
import fs2.Stream
import org.apache.hadoop.fs.Path
import org.apache.parquet.filter2.compat.FilterCompat
import org.apache.parquet.hadoop.{ParquetReader => HadoopParquetReader}

import scala.language.higherKinds

private[parquet4s] object reader {

  def read[F[_]: ContextShift, T: ParquetRecordDecoder](blocker: Blocker,
                                                        path: String,
                                                        options: ParquetReader.Options,
                                                        filter: Filter)
                                                       (implicit F: Sync[F]): Stream[F, T] = {

    for {
      basePath <- Stream.eval(io.makePath(path))
      vcc      <- Stream.eval(F.delay(options.toValueCodecConfiguration))
      decode = (record: RowParquetRecord) => F.delay(ParquetRecordDecoder.decode(record, vcc))
      partitionedDirectory <- io.findPartitionedPaths(blocker, basePath, options.hadoopConf)
      partitionData        <- Stream.eval(F.delay(PartitionFilter.filter(filter, vcc, partitionedDirectory))).flatMap(Stream.iterable)
      (partitionFilter, partitionedPath) = partitionData
      reader <- Stream.resource(readerResource(blocker, partitionedPath.path, options, partitionFilter))
      entity <- readerStream(blocker, reader)
        .evalTap { record =>
          partitionedPath.partitions.traverse_ { case (name, value) =>
            F.delay(record.add(name.split("\\.").toList, BinaryValue(value)))
          }
        }
        .evalMap(decode)
    } yield entity
  }

  private def readerStream[T, F[_] : ContextShift: Sync](blocker: Blocker,
                                                         reader: HadoopParquetReader[RowParquetRecord]
                                                        ): Stream[F, RowParquetRecord] =
    Stream.unfoldEval(reader) { r =>
      blocker.delay(r.read()).map(record => Option(record).map((_, r)))
    }

  private def readerResource[F[_]: Sync: ContextShift](blocker: Blocker,
                                                       path: Path,
                                                       options: ParquetReader.Options,
                                                       filter: FilterCompat.Filter
                                                      ): Resource[F, HadoopParquetReader[RowParquetRecord]] =
    Resource.fromAutoCloseableBlocking(blocker)(
      Sync[F].delay(
        HadoopParquetReader.builder[RowParquetRecord](new ParquetReadSupport(), path)
          .withConf(options.hadoopConf)
          .withFilter(filter)
          .build()
      )
    )

}
