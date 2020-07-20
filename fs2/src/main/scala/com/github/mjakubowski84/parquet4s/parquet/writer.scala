package com.github.mjakubowski84.parquet4s.parquet

import cats.effect.{Blocker, ContextShift, Resource, Sync}
import cats.implicits._
import com.github.mjakubowski84.parquet4s.{ParquetRecordEncoder, ParquetSchemaResolver, ParquetWriter, RowParquetRecord}
import fs2.{Chunk, Pipe, Pull, Stream}
import org.apache.hadoop.fs.Path

import scala.language.higherKinds

private[parquet4s] object writer {

  private class Writer[T, F[_]: Sync: ContextShift](blocker: Blocker,
                                                    internalWriter: ParquetWriter.InternalWriter,
                                                    encode: T => F[RowParquetRecord]
                                                   ) extends AutoCloseable {

    def write(elem: T): F[Unit] =
      for {
        record <- encode(elem)
        _ <- blocker.delay(internalWriter.write(record))
      } yield ()

    def writePull(chunk: Chunk[T]): Pull[F, Nothing, Unit] =
      Pull.eval(chunk.traverse_(write))

    def writeAll(in: Stream[F, T]): Pull[F, Nothing, Unit] =
      in.pull.unconsNonEmpty.flatMap {
        case Some((chunk, tail)) => writePull(chunk) >> writeAll(tail)
        case None                => Pull.done
      }

    override def close(): Unit = internalWriter.close()
  }

  def write[T : ParquetRecordEncoder : ParquetSchemaResolver, F[_]: Sync: ContextShift](blocker: Blocker,
                                                                                        path: String,
                                                                                        options: ParquetWriter.Options
                                                                                       ): Pipe[F, T, Unit] =
    in =>
      for {
        hadoopPath <- Stream.eval(io.makePath(path))
        logger <- Stream.eval(logger(getClass))
        _ <- Stream.eval(io.validateWritePath(blocker, hadoopPath, options, logger))
        writer <- Stream.resource(writerResource[T, F](blocker, hadoopPath, options))
        _ <- writer.writeAll(in).stream
      } yield ()

  private def writerResource[T : ParquetRecordEncoder : ParquetSchemaResolver, F[_]: ContextShift](blocker: Blocker,
                                                                                                   path: Path,
                                                                                                   options: ParquetWriter.Options)
                                                                                                  (implicit F: Sync[F]): Resource[F, Writer[T, F]] =
    Resource.fromAutoCloseableBlocking(blocker)(
      for {
        schema <- F.delay(ParquetSchemaResolver.resolveSchema[T])
        valueCodecConfiguration <- F.delay(options.toValueCodecConfiguration)
        internalWriter <- F.delay(ParquetWriter.internalWriter(path, schema, options))
        encode = { (entity: T) => F.delay(ParquetRecordEncoder.encode[T](entity, valueCodecConfiguration)) }
      } yield new Writer[T, F](blocker, internalWriter, encode)
    )

}
