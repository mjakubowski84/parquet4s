package com.github.mjakubowski84.parquet4s.parquet

import cats.effect.{Resource, Sync}
import cats.implicits._
import com.github.mjakubowski84.parquet4s.{ParquetRecordEncoder, ParquetSchemaResolver, ParquetWriter, RowParquetRecord}
import fs2.{Chunk, Pipe, Pull, Stream}
import org.apache.hadoop.fs.Path

import scala.language.higherKinds

private[parquet4s] object writer {

  private class Writer[T, F[_]](internalWriter: ParquetWriter.InternalWriter,
                                encode: T => F[RowParquetRecord]
                               )(implicit F: Sync[F]) extends AutoCloseable {

    def write(elem: T): F[Unit] =
      for {
        record <- encode(elem)
        // F.delay is much faster when using local file system - let's compare it when using AWS
        _ <- F.blocking(internalWriter.write(record))
      } yield ()

    def writePull(chunk: Chunk[T]): Pull[F, Nothing, Unit] =
      Pull.eval(chunk.traverse_(write))

    def writeAll(in: Stream[F, T]): Pull[F, Nothing, Unit] =
      in.pull.uncons.flatMap {
        case Some((chunk, tail)) => writePull(chunk) >> writeAll(tail)
        case None                => Pull.done
      }

    def writeAllStream(in: Stream[F, T]): Stream[F, fs2.INothing] = writeAll(in).stream

    override def close(): Unit = internalWriter.close()
  }

  def write[T : ParquetRecordEncoder : ParquetSchemaResolver, F[_]: Sync](path: String,
                                                                          options: ParquetWriter.Options
                                                                         ): Pipe[F, T, fs2.INothing] =
    in =>
      for {
        hadoopPath <- Stream.eval(io.makePath(path))
        logger <- Stream.eval(logger(getClass))
        _ <- Stream.eval(io.validateWritePath(hadoopPath, options, logger))
        writer <- Stream.resource(writerResource[T, F](hadoopPath, options))
        nothing <- writer.writeAllStream(in)
      } yield nothing

  private def writerResource[T : ParquetRecordEncoder : ParquetSchemaResolver, F[_]](path: Path,
                                                                                     options: ParquetWriter.Options)
                                                                                    (implicit F: Sync[F]): Resource[F, Writer[T, F]] =
    Resource.fromAutoCloseable(
      for {
        schema <- F.catchNonFatal(ParquetSchemaResolver.resolveSchema[T])
        valueCodecConfiguration <- F.catchNonFatal(options.toValueCodecConfiguration)
        internalWriter <- F.blocking(ParquetWriter.internalWriter(path, schema, options))
        encode = { (entity: T) => F.catchNonFatal(ParquetRecordEncoder.encode[T](entity, valueCodecConfiguration)) }
      } yield new Writer[T, F](internalWriter, encode)
    )

}
