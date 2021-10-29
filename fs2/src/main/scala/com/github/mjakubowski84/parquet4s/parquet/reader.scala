package com.github.mjakubowski84.parquet4s.parquet

import cats.effect.{Resource, Sync}
import cats.implicits.*
import com.github.mjakubowski84.parquet4s.*
import fs2.Stream
import org.apache.parquet.filter2.compat.FilterCompat
import org.apache.parquet.hadoop.ParquetReader as HadoopParquetReader
import org.apache.parquet.schema.MessageType

import scala.language.higherKinds

object reader {

  /** Factory of builders of Parquet readers.
    */
  trait FromParquet[F[_]] {

    /** Creates [[Builder]] of Parquet reader for documents of type <i>T</i>.
      */
    def as[T: ParquetRecordDecoder]: Builder[F, T]

    /** Creates [[Builder]] of Parquet reader for <i>projected</i> documents of type <i>T</i>. Due to projection reader
      * does not attempt to read all existing columns of the file but applies enforced projection schema.
      */
    def projectedAs[T: ParquetRecordDecoder: ParquetSchemaResolver]: Builder[F, T]

    /** Creates [[Builder]] of Parquet reader of generic records.
      */
    def generic: Builder[F, RowParquetRecord]

    /** Creates [[Builder]] of Parquet reader of <i>projected</i> generic records. Due to projection reader does not
      * attempt to read all existing columns of the file but applies enforced projection schema.
      */
    def projectedGeneric(projectedSchema: MessageType): Builder[F, RowParquetRecord]
  }

  private[parquet4s] class FromParquetImpl[F[_]: Sync] extends FromParquet[F] {
    override def as[T: ParquetRecordDecoder]: Builder[F, T] = BuilderImpl()
    override def projectedAs[T: ParquetRecordDecoder: ParquetSchemaResolver]: Builder[F, T] = BuilderImpl(
      projectedSchemaResolverOpt = Option(implicitly[ParquetSchemaResolver[T]])
    )
    override def generic: Builder[F, RowParquetRecord] = BuilderImpl[F, RowParquetRecord]()
    override def projectedGeneric(projectedSchema: MessageType): Builder[F, RowParquetRecord] =
      BuilderImpl[F, RowParquetRecord](
        projectedSchemaResolverOpt = Option(RowParquetRecord.genericParquetSchemaResolver(projectedSchema))
      )
  }

  trait Builder[F[_], T] {

    /** @param options
      *   configuration of how Parquet files should be read
      */
    def options(options: ParquetReader.Options): Builder[F, T]

    /** @param filter
      *   optional before-read filter; no filtering is applied by default; check [[Filter]] for more details
      */
    def filter(filter: Filter): Builder[F, T]

    /** @param path
      *   [[Path]] to Parquet files, e.g.: {{{Path("file:///data/users")}}}
      * @return
      *   final [[fs2.Stream]]
      */
    def read(path: Path): Stream[F, T]
  }

  private case class BuilderImpl[F[_]: Sync, T: ParquetRecordDecoder](
      options: ParquetReader.Options                               = ParquetReader.Options(),
      filter: Filter                                               = Filter.noopFilter,
      projectedSchemaResolverOpt: Option[ParquetSchemaResolver[T]] = None
  ) extends Builder[F, T] {
    override def options(options: ParquetReader.Options): Builder[F, T] = this.copy(options = options)

    override def filter(filter: Filter): Builder[F, T] = this.copy(filter = filter)

    override def read(path: Path): Stream[F, T] = reader.read(path, options, filter, projectedSchemaResolverOpt)
  }

  private[parquet4s] def read[F[_], T: ParquetRecordDecoder](
      basePath: Path,
      options: ParquetReader.Options,
      filter: Filter,
      projectedSchemaResolverOpt: Option[ParquetSchemaResolver[T]]
  )(implicit F: Sync[F]): Stream[F, T] =
    for {
      vcc <- Stream.eval(F.pure(ValueCodecConfiguration(options)))
      decode = (record: RowParquetRecord) => F.catchNonFatal(ParquetRecordDecoder.decode(record, vcc))
      partitionedDirectory <- io.findPartitionedPaths(basePath, options.hadoopConf)
      projectedSchemaOpt <- Stream.eval(
        projectedSchemaResolverOpt
          .traverse(implicit resolver =>
            F.catchNonFatal(ParquetSchemaResolver.resolveSchema(partitionedDirectory.schema))
          )
      )
      partitionData <- Stream
        .eval(F.catchNonFatal(PartitionFilter.filter(filter, vcc, partitionedDirectory)))
        .flatMap(Stream.iterable)
      (partitionFilter, partitionedPath) = partitionData
      reader <- Stream.resource(readerResource(partitionedPath.path, options, partitionFilter, projectedSchemaOpt))
      entity <- readerStream(reader)
        .evalMap { record =>
          partitionedPath.partitions.foldLeft(F.pure(record)) { case (f, (columnPath, value)) =>
            f.flatMap { r =>
              F.catchNonFatal(r.updated(columnPath, BinaryValue(value)))
            }
          }
        }
        .evalMap(decode)
    } yield entity

  private def readerStream[T, F[_]: Sync](reader: HadoopParquetReader[RowParquetRecord])(implicit
      F: Sync[F]
  ): Stream[F, RowParquetRecord] =
    Stream.repeatEval(F.blocking(reader.read())).takeWhile(_ != null)

  private def readerResource[F[_]: Sync](
      path: Path,
      options: ParquetReader.Options,
      filter: FilterCompat.Filter,
      projectionSchemaOpt: Option[MessageType]
  ): Resource[F, HadoopParquetReader[RowParquetRecord]] =
    Resource.fromAutoCloseable(
      Sync[F].blocking(
        HadoopParquetReader
          .builder[RowParquetRecord](new ParquetReadSupport(projectionSchemaOpt), path.toHadoop)
          .withConf(options.hadoopConf)
          .withFilter(filter)
          .build()
      )
    )

}
