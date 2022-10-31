package com.github.mjakubowski84.parquet4s.parquet

import cats.effect.{Resource, Sync}
import cats.implicits.*
import com.github.mjakubowski84.parquet4s.{
  ParquetRecordEncoder,
  ParquetSchemaResolver,
  ParquetWriter,
  Path,
  RowParquetRecord,
  ValueCodecConfiguration
}
import fs2.{Chunk, Pipe, Pull, Stream}
import org.apache.parquet.schema.MessageType

import scala.language.higherKinds

private[parquet4s] object writer {

  trait ToParquet[F[_]] {

    /** Creates a builder of pipe that processes data of given type
      * @tparam T
      *   Schema type
      */
    def of[T: ParquetSchemaResolver: ParquetRecordEncoder]: Builder[F, T]

    /** Creates a builder of pipe that processes generic records
      */
    def generic(schema: MessageType): Builder[F, RowParquetRecord]
  }

  private[parquet4s] class ToParquetImpl[F[_]: Sync] extends ToParquet[F] {
    override def of[T: ParquetSchemaResolver: ParquetRecordEncoder]: Builder[F, T] =
      BuilderImpl()
    override def generic(schema: MessageType): Builder[F, RowParquetRecord] =
      BuilderImpl()(
        schemaResolver = RowParquetRecord.genericParquetSchemaResolver(schema),
        encoder        = RowParquetRecord.genericParquetRecordEncoder,
        sync           = Sync[F]
      )
  }

  trait Builder[F[_], T] {

    /** @param options
      *   writer options
      */
    def options(options: ParquetWriter.Options): Builder[F, T]

    /** @param path
      *   at which data is supposed to be written
      * @return
      *   final [[fs2.Pipe]]
      */
    def write(path: Path): Pipe[F, T, Nothing]
  }

  private case class BuilderImpl[F[_], T](options: ParquetWriter.Options = ParquetWriter.Options())(implicit
      schemaResolver: ParquetSchemaResolver[T],
      encoder: ParquetRecordEncoder[T],
      sync: Sync[F]
  ) extends Builder[F, T] {
    override def options(options: ParquetWriter.Options): Builder[F, T] = this.copy(options = options)
    override def write(path: Path): Pipe[F, T, Nothing]                 = pipe[F, T](path, options)
  }

  private class Writer[T, F[_]](internalWriter: ParquetWriter.InternalWriter, encode: T => F[RowParquetRecord])(implicit
      F: Sync[F]
  ) extends AutoCloseable {

    def write(elem: T): F[Unit] =
      for {
        record <- encode(elem)
        _      <- F.delay(scala.concurrent.blocking(internalWriter.write(record)))
      } yield ()

    def writePull(chunk: Chunk[T]): Pull[F, Nothing, Unit] =
      Pull.eval(chunk.traverse_(write))

    def writeAll(in: Stream[F, T]): Pull[F, Nothing, Unit] =
      in.pull.uncons.flatMap {
        case Some((chunk, tail)) => writePull(chunk) >> writeAll(tail)
        case None                => Pull.done
      }

    def writeAllStream(in: Stream[F, T]): Stream[F, Nothing] = writeAll(in).stream

    override def close(): Unit =
      try internalWriter.close()
      catch {
        case _: NullPointerException => // ignores bug in Parquet
      }
  }

  private def pipe[F[_]: Sync, T: ParquetRecordEncoder: ParquetSchemaResolver](
      path: Path,
      options: ParquetWriter.Options
  ): Pipe[F, T, Nothing] =
    in =>
      for {
        logger  <- Stream.eval(logger(getClass))
        _       <- Stream.eval(io.validateWritePath(path, options, logger))
        writer  <- Stream.resource(writerResource[T, F](path, options))
        nothing <- writer.writeAllStream(in)
      } yield nothing

  private def writerResource[T: ParquetRecordEncoder: ParquetSchemaResolver, F[_]](
      path: Path,
      options: ParquetWriter.Options
  )(implicit F: Sync[F]): Resource[F, Writer[T, F]] =
    Resource.fromAutoCloseable(
      for {
        valueCodecConfiguration <- F.catchNonFatal(ValueCodecConfiguration(options))
        schema                  <- F.catchNonFatal(ParquetSchemaResolver.resolveSchema[T])
        internalWriter <- F.delay(scala.concurrent.blocking(ParquetWriter.internalWriter(path, schema, options)))
        encode = { (entity: T) => F.catchNonFatal(ParquetRecordEncoder.encode[T](entity, valueCodecConfiguration)) }
      } yield new Writer[T, F](internalWriter, encode)
    )

}
