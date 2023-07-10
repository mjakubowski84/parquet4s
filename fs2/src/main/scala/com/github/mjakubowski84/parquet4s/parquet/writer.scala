package com.github.mjakubowski84.parquet4s.parquet

import cats.effect.{Resource, Sync}
import cats.implicits.*
import com.github.mjakubowski84.parquet4s.{
  ParquetRecordEncoder,
  ParquetSchemaResolver,
  ParquetWriter,
  Path,
  RowParquetRecord,
  ValueCodecConfiguration,
  experimental
}
import fs2.{Chunk, Pipe, Pull, Stream}
import org.apache.parquet.hadoop.ParquetWriter as HadoopParquetWriter
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

    /** Creates a builder of pipe that processes data of a given type using a
      * [[org.apache.parquet.hadoop.ParquetWriter]] built from a provided
      * [[org.apache.parquet.hadoop.ParquetWriter.Builder]].
      * @tparam T
      *   Schema type
      * @tparam B
      *   Type of custom [[org.apache.parquet.hadoop.ParquetWriter.Builder]]
      */
    @experimental
    def custom[T, B <: HadoopParquetWriter.Builder[T, B]](builder: B): CustomBuilder[F, T]
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
    override def custom[T, B <: HadoopParquetWriter.Builder[T, B]](builder: B): CustomBuilder[F, T] =
      CustomBuilderImpl(builder)
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
    override def write(path: Path): Pipe[F, T, Nothing]                 = rowParquetRecordPipe[F, T](path, options)
  }

  trait CustomBuilder[F[_], T] {

    /** @param options
      *   writer options
      */
    def options(options: ParquetWriter.Options): CustomBuilder[F, T]

    /** @return
      *   final [[fs2.Pipe]]
      */
    def write: Pipe[F, T, Nothing]
  }

  private case class CustomBuilderImpl[F[_]: Sync, T, B <: HadoopParquetWriter.Builder[T, B]](
      builder: B,
      maybeOptions: Option[ParquetWriter.Options] = None
  ) extends CustomBuilder[F, T] {
    override def options(options: ParquetWriter.Options): CustomBuilder[F, T] =
      this.copy(maybeOptions = Some(options))

    override def write: Pipe[F, T, Nothing] =
      pipe(
        maybeOptions
          .fold(builder)(_.applyTo[T, B](builder))
          .build()
      )
  }

  private class Writer[T, F[_]](internalWriter: HadoopParquetWriter[T])(implicit F: Sync[F]) extends AutoCloseable {
    def write(elem: T): F[Unit] =
      F.delay(scala.concurrent.blocking(internalWriter.write(elem)))

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
  private object Writer {
    def apply[T, F[_]](makeParquetWriter: => HadoopParquetWriter[T])(implicit
        F: Sync[F]
    ): Resource[F, Writer[T, F]] =
      Resource.fromAutoCloseable(
        F.blocking(makeParquetWriter).map(new Writer[T, F](_))
      )
  }

  private def rowParquetRecordPipe[F[_], T: ParquetRecordEncoder: ParquetSchemaResolver](
      path: Path,
      options: ParquetWriter.Options
  )(implicit F: Sync[F]): Pipe[F, T, Nothing] = { in =>
    val valueCodecConfiguration = ValueCodecConfiguration(options)
    in
      .evalMapChunk(entity => F.catchNonFatal(ParquetRecordEncoder.encode[T](entity, valueCodecConfiguration)))
      .through(
        pipe(ParquetWriter.internalWriter(path.toOutputFile(options), ParquetSchemaResolver.resolveSchema[T], options))
      )
  }

  private def pipe[F[_]: Sync, T](makeParquetWriter: => HadoopParquetWriter[T]): Pipe[F, T, Nothing] =
    in =>
      Stream
        .resource(Writer[T, F](makeParquetWriter))
        .flatMap(_.writeAllStream(in))
}
