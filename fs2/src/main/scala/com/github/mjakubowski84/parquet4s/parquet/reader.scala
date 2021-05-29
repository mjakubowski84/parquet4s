package com.github.mjakubowski84.parquet4s.parquet

import cats.effect.{Resource, Sync}
import cats.implicits._
import com.github.mjakubowski84.parquet4s._
import fs2.Stream
import org.apache.parquet.filter2.compat.FilterCompat
import org.apache.parquet.hadoop.{ParquetReader => HadoopParquetReader}
import org.apache.parquet.schema.MessageType

import scala.language.higherKinds

object reader {

  object Builder {
    private[parquet4s] def apply[F[_], T](): Builder[F, T] = BuilderImpl(
      options = ParquetReader.Options(),
      filter = Filter.noopFilter,
      schemaResolverOpt = None
    )
  }

  trait Builder[F[_], T] {
    /**
     * @param options configuration of how Parquet files should be read
     */
    def options(options: ParquetReader.Options): Builder[F, T]
    /**
     * @param filter optional before-read filter; no filtering is applied by default; check [[Filter]] for more details
     */
    def filter(filter: Filter): Builder[F, T]
    /**
     * @param schemaResolver resolved schema that is going to be used as a projection over original file schema
     */
    def projection(implicit schemaResolver: ParquetSchemaResolver[T]): Builder[F, T]
    /**
     * @param path [[Path]] to Parquet files, e.g.: {{{ Path("file:///data/users") }}}
     * @param decoder decodes [[RowParquetRecord]] to your data type
     * @param F [[cats.effect.Sync!]] monad
     * @return final [[fs2.Stream!]]
     */
    def read(path: Path)
            (implicit decoder: ParquetRecordDecoder[T], F: Sync[F]): Stream[F, T]
  }

  private case class BuilderImpl[F[_], T](options: ParquetReader.Options,
                                          filter: Filter,
                                          schemaResolverOpt: Option[ParquetSchemaResolver[T]]
                                         ) extends Builder[F, T] {
    override def options(options: ParquetReader.Options): Builder[F, T] =
      this.copy(options = options)

    override def filter(filter: Filter): Builder[F, T] =
      this.copy(filter = filter)

    override def projection(implicit schemaResolver: ParquetSchemaResolver[T]): Builder[F, T] =
      this.copy(schemaResolverOpt = Option(schemaResolver))

    override def read(path: Path)
                     (implicit decoder: ParquetRecordDecoder[T], F: Sync[F]): Stream[F, T] = {
      for {
        projectionSchemaOpt <- Stream.eval(schemaResolverOpt.traverse(implicit resolver => F.catchNonFatal(ParquetSchemaResolver.resolveSchema)))
        readerStream <- reader.read(path, options, filter, projectionSchemaOpt)
      } yield readerStream
    }
  }

  private[parquet4s] def read[F[_], T: ParquetRecordDecoder](basePath: Path,
                                                             options: ParquetReader.Options,
                                                             filter: Filter,
                                                             projectionSchemaOpt: Option[MessageType]
                                                            )(implicit F: Sync[F]): Stream[F, T] = {

    for {
      vcc                  <- Stream.eval(F.pure(ValueCodecConfiguration(options)))
      decode = (record: RowParquetRecord) => F.catchNonFatal(ParquetRecordDecoder.decode(record, vcc))
      partitionedDirectory <- io.findPartitionedPaths(basePath, options.hadoopConf)
      partitionData        <- Stream.eval(F.catchNonFatal(PartitionFilter.filter(filter, vcc, partitionedDirectory)))
                                .flatMap(Stream.iterable)
      (partitionFilter, partitionedPath) = partitionData
      reader <- Stream.resource(readerResource(partitionedPath.path, options, partitionFilter, projectionSchemaOpt))
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
  }

  private def readerStream[T, F[_] : Sync](reader: HadoopParquetReader[RowParquetRecord])
                                          (implicit F: Sync[F]): Stream[F, RowParquetRecord] = {
    // TODO test using AWS in order to check the performance when using delay instead of blocking
    Stream.repeatEval(F.blocking(reader.read())).takeWhile(_ != null)
  }

  private def readerResource[F[_]: Sync](path: Path,
                                         options: ParquetReader.Options,
                                         filter: FilterCompat.Filter,
                                         projectionSchemaOpt: Option[MessageType]
                                        ): Resource[F, HadoopParquetReader[RowParquetRecord]] =
    Resource.fromAutoCloseable(
      Sync[F].blocking(
        HadoopParquetReader.builder[RowParquetRecord](new ParquetReadSupport(projectionSchemaOpt), path.toHadoop)
          .withConf(options.hadoopConf)
          .withFilter(filter)
          .build()
      )
    )

}
