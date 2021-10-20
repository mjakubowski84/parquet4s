package com.github.mjakubowski84.parquet4s.parquet

import cats.effect.{Blocker, ContextShift, Resource, Sync}
import cats.implicits._
import com.github.mjakubowski84.parquet4s._
import fs2.Stream
import org.apache.hadoop.fs.Path
import org.apache.parquet.filter2.compat.FilterCompat
import org.apache.parquet.hadoop.{ParquetReader => HadoopParquetReader}
import org.apache.parquet.schema.MessageType

import scala.language.higherKinds

object reader {

  object Builder {
    private[parquet4s] def apply[F[_], T](): Builder[F, T] = BuilderImpl(
      options                    = ParquetReader.Options(),
      filter                     = Filter.noopFilter,
      projectedSchemaResolverOpt = None
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

    /** @param schemaResolver
      *   resolved schema that is going to be used as a projection over original file schema
      */
    def projection(implicit schemaResolver: SkippingParquetSchemaResolver[T]): Builder[F, T]

    /** @param blocker
      *   Execution context for blocking operations
      * @param path
      *   URI to Parquet files, e.g.: {{{"file:///data/users"}}}
      * @param decoder
      *   decodes [[RowParquetRecord]] to your data type
      * @param F
      *   [[cats.effect.Sync!]] monad
      * @param cs
      *   [[cats.effect.ContextShift!]]
      * @return
      *   final [[fs2.Stream!]]
      */
    def read(blocker: Blocker, path: String)(implicit
        decoder: ParquetRecordDecoder[T],
        F: Sync[F],
        cs: ContextShift[F]
    ): Stream[F, T]
  }

  private case class BuilderImpl[F[_], T](
      options: ParquetReader.Options,
      filter: Filter,
      projectedSchemaResolverOpt: Option[SkippingParquetSchemaResolver[T]]
  ) extends Builder[F, T] {
    override def options(options: ParquetReader.Options): Builder[F, T] =
      this.copy(options = options)

    override def filter(filter: Filter): Builder[F, T] =
      this.copy(filter = filter)

    override def projection(implicit schemaResolver: SkippingParquetSchemaResolver[T]): Builder[F, T] =
      this.copy(projectedSchemaResolverOpt = Option(schemaResolver))

    override def read(blocker: Blocker, path: String)(implicit
        decoder: ParquetRecordDecoder[T],
        F: Sync[F],
        cs: ContextShift[F]
    ): Stream[F, T] =
      reader.read(blocker, path, options, filter, projectedSchemaResolverOpt)
  }

  private[parquet4s] def read[F[_]: ContextShift, T: ParquetRecordDecoder](
      blocker: Blocker,
      path: String,
      options: ParquetReader.Options,
      filter: Filter,
      projectedSchemaResolverOpt: Option[SkippingParquetSchemaResolver[T]]
  )(implicit F: Sync[F]): Stream[F, T] =
    for {
      basePath <- Stream.eval(io.makePath(path))
      vcc      <- Stream.eval(F.delay(options.toValueCodecConfiguration))
      decode = (record: RowParquetRecord) => F.delay(ParquetRecordDecoder.decode(record, vcc))
      partitionedDirectory <- io.findPartitionedPaths(blocker, basePath, options.hadoopConf)
      projectedSchemaOpt <- Stream.eval(
        projectedSchemaResolverOpt
          .traverse(implicit resolver =>
            F.delay(SkippingParquetSchemaResolver.resolveSchema(partitionedDirectory.schema))
          )
      )
      partitionData <- Stream
        .eval(F.delay(PartitionFilter.filter(filter, vcc, partitionedDirectory)))
        .flatMap(Stream.iterable)
      (partitionFilter, partitionedPath) = partitionData
      reader <- Stream.resource(
        readerResource(blocker, partitionedPath.path, options, partitionFilter, projectedSchemaOpt)
      )
      entity <- readerStream(blocker, reader)
        .evalTap { record =>
          partitionedPath.partitions.traverse_ { case (name, value) =>
            F.delay(record.add(name.split("\\.").toList, BinaryValue(value)))
          }
        }
        .evalMap(decode)
    } yield entity

  private def readerStream[T, F[_]: ContextShift: Sync](
      blocker: Blocker,
      reader: HadoopParquetReader[RowParquetRecord]
  ): Stream[F, RowParquetRecord] =
    Stream.unfoldEval(reader) { r =>
      blocker.delay(r.read()).map(record => Option(record).map((_, r)))
    }

  private def readerResource[F[_]: Sync: ContextShift](
      blocker: Blocker,
      path: Path,
      options: ParquetReader.Options,
      filter: FilterCompat.Filter,
      projectionSchemaOpt: Option[MessageType]
  ): Resource[F, HadoopParquetReader[RowParquetRecord]] =
    Resource.fromAutoCloseableBlocking(blocker)(
      Sync[F].delay(
        HadoopParquetReader
          .builder[RowParquetRecord](new ParquetReadSupport(projectionSchemaOpt), path)
          .withConf(options.hadoopConf)
          .withFilter(filter)
          .build()
      )
    )

}
