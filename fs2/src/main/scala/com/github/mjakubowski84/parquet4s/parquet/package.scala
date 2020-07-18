package com.github.mjakubowski84.parquet4s

import cats.effect.{Resource, Sync}
import fs2.{Chunk, Pipe, Pull, Stream}
import org.apache.hadoop.fs.Path
import cats.implicits._
import org.apache.parquet.hadoop.{ParquetReader => HadoopParquetReader}

import scala.language.higherKinds

package object parquet {

  private class Writer[T, F[_]](internalWriter: ParquetWriter.InternalWriter, encode: T => RowParquetRecord)
                               (implicit F: Sync[F]) extends AutoCloseable {

    def write(elem: T): F[Writer[T, F]] =
      for {
        record <- F.delay(encode(elem))
        _ <- F.delay(internalWriter.write(record))
      } yield this

    def writePull(chunk: Chunk[T]): Pull[F, Nothing, Writer[T, F]] =
      Pull.eval(chunk.foldM(this)(_.write(_)))

    def writeAll(in: Stream[F, T]): Pull[F, Nothing, Writer[T, F]] = {
      in.pull.uncons.flatMap {
        case Some((chunk, tail)) => writePull(chunk).flatMap(_.writeAll(tail))
        case None                => Pull.pure(this)
      }
    }

    override def close(): Unit = internalWriter.close()
  }

  private def writerResource[T : ParquetRecordEncoder : ParquetSchemaResolver, F[_]](path: String, options: ParquetWriter.Options)
                                                                                    (implicit F: Sync[F]): Resource[F, Writer[T, F]] =
      Resource.fromAutoCloseable(
        for {
          valueCodecConfiguration <- F.delay(options.toValueCodecConfiguration)
          schema <- F.delay(ParquetSchemaResolver.resolveSchema[T])
          internalWriter <- F.delay(ParquetWriter.internalWriter(new Path(path), schema, options))
          encode = { (entity: T) => ParquetRecordEncoder.encode[T](entity, valueCodecConfiguration) }
        } yield new Writer[T, F](internalWriter, encode)
      )

  def writeSingleFile[T : ParquetRecordEncoder : ParquetSchemaResolver, F[_]: Sync](path: String,
                                                                                    options: ParquetWriter.Options = ParquetWriter.Options()
                                                                                   ): Pipe[F, T, Unit] =
    in =>
      Stream
        .resource(writerResource[T, F](path, options))
        .flatMap(_.writeAll(in).void.stream)

  private def readerResource[F[_]](path: String,
                             options: ParquetReader.Options,
                             filter: Filter
                            )(implicit F: Sync[F]): Resource[F, HadoopParquetReader[RowParquetRecord]] =
    Resource.fromAutoCloseable(
      F.delay(
        HadoopParquetReader.builder[RowParquetRecord](new ParquetReadSupport(), new Path(path))
          .withConf(options.hadoopConf)
          .withFilter(filter.toFilterCompat(options.toValueCodecConfiguration))
          .build())
    )

  def read[T: ParquetRecordDecoder, F[_]: Sync](
                                                 path: String,
                                                 options: ParquetReader.Options = ParquetReader.Options(),
                                                 filter: Filter = Filter.noopFilter
                                   ): Stream[F, T] = {
    val vcc = options.toValueCodecConfiguration
    val decode = (record: RowParquetRecord) => Sync[F].delay(ParquetRecordDecoder.decode(record, vcc))
    Stream.resource(readerResource(path, options, filter)).flatMap { reader =>
      Stream.unfoldEval(reader) { r =>
        Sync[F].delay(r.read()).map(record => Option(record).map((_, r)))
      }.evalMap(decode)
    }
  }

}
