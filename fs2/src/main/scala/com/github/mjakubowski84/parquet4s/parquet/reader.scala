package com.github.mjakubowski84.parquet4s.parquet

import cats.effect.{Concurrent, Resource, Sync}
import cats.implicits.*
import com.github.mjakubowski84.parquet4s.*
import fs2.Stream
import org.apache.parquet.filter2.compat.FilterCompat
import org.apache.parquet.schema.{MessageType, Type}
import org.apache.parquet.hadoop.util.HadoopInputFile
import org.apache.parquet.io.InputFile

object reader {

  val DefaultChunkSize = 16

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

    // format: off
    /** Creates [[Builder]] of Parquet reader returning <i>projected</i> generic records. Due to projection, reader does
     * not attempt to read all existing columns of the file but applies enforced projection schema. Besides simple
     * projection one can use aliases and extract values from nested fields - in a way similar to SQL.
     * <br/> <br/>
     * @example
     *   <pre> 
     *projectedGeneric(
     *  Col("foo").as[Int], // selects Int column "foo"
     *  Col("bar.baz".as[String]), // selects String field "bar.baz", creates column "baz" wih a value of "baz"
     *  Col("bar.baz".as[String].alias("bar_baz")) // selects String field "bar.baz", creates column "bar_baz" wih a value of "baz"
     *)
     *   </pre>  
     * @param col
     *   first column projection
     * @param cols
     *   next column projections
     */
    // format: on  
    def projectedGeneric(col: TypedColumnPath[?], cols: TypedColumnPath[?]*): Builder[F, RowParquetRecord]

    /** Creates [[CustomBuilder]] of Parquet reader data using custom internal implementation.
      */
    @experimental
    def custom[T](builder: org.apache.parquet.hadoop.ParquetReader.Builder[T]): CustomBuilder[F, T]

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
    override def projectedGeneric(col: TypedColumnPath[?], cols: TypedColumnPath[?]*): Builder[F, RowParquetRecord] = {
      val (fields, columnProjections) =
        (col +: cols.toVector).zipWithIndex
          .foldLeft((Vector.empty[Type], Vector.empty[ColumnProjection])) {
            case ((fields, projections), (columnPath, ordinal)) =>
              val updatedFields      = fields :+ columnPath.toType
              val updatedProjections = projections :+ ColumnProjection(columnPath, ordinal)
              updatedFields -> updatedProjections
          }
      BuilderImpl[F, RowParquetRecord](
        projectedSchemaResolverOpt = Option(new LazyParquetSchemaResolver(Message.merge(fields))),
        columnProjections          = columnProjections
      )
    }

    override def custom[T](builder: org.apache.parquet.hadoop.ParquetReader.Builder[T]): CustomBuilder[F, T] =
      CustomBuilderImpl(builder)
  }

  private class LazyParquetSchemaResolver[T](messageSchema: => MessageType) extends ParquetSchemaResolver[T] {
    private lazy val wrapped                = RowParquetRecord.genericParquetSchemaResolver(messageSchema)
    override def schemaName: Option[String] = wrapped.schemaName
    override def resolveSchema(cursor: Cursor): List[Type] = wrapped.resolveSchema(cursor)
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

    /** For sake of better performance reader processes records in chunks. Default value is `16`.
      * @param chunkSize
      *   default value override
      */
    def chunkSize(chunkSize: Int): Builder[F, T]

    /** How many files at most shall be read in parallel. Default value is `1`.
      * @param n
      *   desired parallelism
      * @param concurrent
      *   parallel reading requires concurrency
      */
    def parallelism(n: Int)(implicit concurrent: Concurrent[F]): Builder[F, T]

    /** @param path
      *   [[Path]] to Parquet files, e.g.: {{{Path("file:///data/users")}}}
      * @return
      *   final [[fs2.Stream]]
      */
    def read(path: Path): Stream[F, T]

    /** @param path
      *   [[Path]] to Parquet files, e.g.: {{{Path("file:///data/users")}}}
      * @return
      *   [[fs2.Stream]] of streams of content of each individual Parquet file
      */
    def readFileStreams(path: Path): Stream[F, Stream[F, T]]

    /** @param inputFile
      *   file to read
      * @return
      *   final [[fs2.Stream]]
      */
    @experimental
    def read(inputFile: InputFile): Stream[F, T]

    /** @param inputFile
      *   file to read
      * @return
      *   [[fs2.Stream]] of streams of content of each individual Parquet file
      */
    @experimental
    def readFileStreams(inputFile: InputFile): Stream[F, Stream[F, T]]
  }

  private case class BuilderImpl[F[_]: Sync, T: ParquetRecordDecoder](
      options: ParquetReader.Options                               = ParquetReader.Options(),
      filter: Filter                                               = Filter.noopFilter,
      chunkSize: Int                                               = DefaultChunkSize,
      parallelism: Option[(Int, Concurrent[F])]                    = None,
      projectedSchemaResolverOpt: Option[ParquetSchemaResolver[T]] = None,
      columnProjections: Seq[ColumnProjection]                     = Seq.empty
  ) extends Builder[F, T] {
    override def options(options: ParquetReader.Options): Builder[F, T] = this.copy(options = options)

    override def filter(filter: Filter): Builder[F, T] = this.copy(filter = filter)

    override def chunkSize(chunkSize: Int): Builder[F, T] = this.copy(chunkSize = chunkSize)

    override def parallelism(n: Int)(implicit concurrent: Concurrent[F]): Builder[F, T] =
      this.copy(parallelism = Some(n -> concurrent))

    override def read(path: Path): Stream[F, T] =
      read(path.toInputFile(options))

    def readFileStreams(path: Path): Stream[F, Stream[F, T]] =
      readFileStreams(path.toInputFile(options))

    override def read(inputFile: InputFile): Stream[F, T] =
      reader.read(inputFile, options, filter, chunkSize, parallelism, projectedSchemaResolverOpt, columnProjections)

    override def readFileStreams(inputFile: InputFile): Stream[F, Stream[F, T]] =
      reader.readStreamOfFiles(
        inputFile,
        options,
        filter,
        chunkSize,
        projectedSchemaResolverOpt,
        columnProjections
      )
  }

  @experimental
  trait CustomBuilder[F[_], T] {

    /** @param options
      *   configuration of how Parquet files should be read
      */
    def options(options: ParquetReader.Options): CustomBuilder[F, T]

    /** @param filter
      *   optional before-read filter; no filtering is applied by default; check [[Filter]] for more details
      */
    def filter(filter: Filter): CustomBuilder[F, T]

    /** For sake of better performance reader processes records in chunks. Default value is `16`.
      *
      * @param chunkSize
      *   default value override
      */
    def chunkSize(chunkSize: Int): CustomBuilder[F, T]

    /** @return
      *   final [[fs2.Stream]]
      */
    def read: Stream[F, T]

  }

  private case class CustomBuilderImpl[F[_]: Sync, T](
      builder: org.apache.parquet.hadoop.ParquetReader.Builder[T],
      options: ParquetReader.Options = ParquetReader.Options(),
      filter: Filter                 = Filter.noopFilter,
      chunkSize: Int                 = DefaultChunkSize
  ) extends CustomBuilder[F, T] {
    override def options(options: ParquetReader.Options): CustomBuilder[F, T] = this.copy(options = options)

    override def filter(filter: Filter): CustomBuilder[F, T] = this.copy(filter = filter)

    override def chunkSize(chunkSize: Int): CustomBuilder[F, T] = this.copy(chunkSize = chunkSize)

    override def read: Stream[F, T] = {
      val parquetIteratorResource = Resource.fromAutoCloseable(
        Sync[F].blocking(
          new ParquetIterator[T](
            options.applyTo(builder).withFilter(filter.toFilterCompat(ValueCodecConfiguration(options)))
          )
        )
      )

      for {
        parquetIterator <- Stream.resource(parquetIteratorResource)
        stream          <- Stream.fromBlockingIterator[F](parquetIterator, chunkSize)
      } yield stream
    }

  }

  private[parquet4s] def read[F[_], T](
      inputFile: InputFile,
      options: ParquetReader.Options,
      filter: Filter,
      chunkSize: Int,
      parallelism: Option[(Int, Concurrent[F])],
      projectedSchemaResolverOpt: Option[ParquetSchemaResolver[T]],
      columnProjections: Seq[ColumnProjection]
  )(implicit F: Sync[F], decoder: ParquetRecordDecoder[T]): Stream[F, T] = {
    val streamOfFiles =
      readStreamOfFiles(inputFile, options, filter, chunkSize, projectedSchemaResolverOpt, columnProjections)
    parallelism.fold(streamOfFiles.flatten) { case (maxOpen, concurrent) =>
      streamOfFiles.parJoin(maxOpen)(concurrent)
    }
  }

  private[parquet4s] def readStreamOfFiles[F[_], T](
      inputFile: InputFile,
      options: ParquetReader.Options,
      filter: Filter,
      chunkSize: Int,
      projectedSchemaResolverOpt: Option[ParquetSchemaResolver[T]],
      columnProjections: Seq[ColumnProjection]
  )(implicit F: Sync[F], decoder: ParquetRecordDecoder[T]): Stream[F, Stream[F, T]] = {
    val vcc    = ValueCodecConfiguration(options)
    val decode = (record: RowParquetRecord) => F.delay(decoder.decode(record, vcc))

    val streamOfStreamsParquetRecord = inputFile match {
      case hadoopInputFile: HadoopInputFile =>
        readMultipleFiles[F, T](
          basePath                   = Path(hadoopInputFile.getPath),
          options                    = options,
          filter                     = filter,
          projectedSchemaResolverOpt = projectedSchemaResolverOpt,
          columnProjections          = columnProjections,
          vcc                        = vcc,
          chunkSize                  = chunkSize,
          metadataReader             = decoder
        )
      case _ =>
        readSingleFile(
          inputFile                  = inputFile,
          filter                     = filter,
          projectedSchemaResolverOpt = projectedSchemaResolverOpt,
          columnProjections          = columnProjections,
          vcc                        = vcc,
          chunkSize                  = chunkSize,
          metadataReader             = decoder,
          options                    = options
        )
    }

    streamOfStreamsParquetRecord.map(_.evalMapChunk(decode))
  }

  private def readMultipleFiles[F[_], T](
      basePath: Path,
      options: ParquetReader.Options,
      filter: Filter,
      projectedSchemaResolverOpt: Option[ParquetSchemaResolver[T]],
      columnProjections: Seq[ColumnProjection],
      vcc: ValueCodecConfiguration,
      chunkSize: Int,
      metadataReader: MetadataReader
  )(implicit F: Sync[F]): Stream[F, Stream[F, RowParquetRecord]] = {
    val fallbackFilterCompat = F.delay(filter.toNonPredicateFilterCompat)
    for {
      logger               <- Stream.eval(logger[F](this.getClass))
      partitionedDirectory <- io.listPartitionedDirectory(basePath, options.hadoopConf, logger, filter, vcc)
      projectedSchemaOpt <- Stream.eval(
        projectedSchemaResolverOpt
          .traverse(implicit resolver =>
            F.catchNonFatal(ParquetSchemaResolver.resolveSchema(toSkip = partitionedDirectory.schema))
          )
      )
      partitionedPath <- Stream.iterable(partitionedDirectory.paths)
      partitionFilter <- Stream.eval(
        partitionedPath.filterPredicateOpt.fold(fallbackFilterCompat)(pathFilterPredicate =>
          F.catchNonFatal(FilterCompat.get(pathFilterPredicate))
        )
      )
      parquetIterator <- Stream.resource(
        parquetIteratorResource(
          inputFile           = partitionedPath.inputFile,
          filter              = partitionFilter,
          projectionSchemaOpt = projectedSchemaOpt,
          columnProjections   = columnProjections,
          metadataReader      = metadataReader,
          options             = options
        )
      )
    } yield partitionedReaderStream[F](parquetIterator, partitionedPath, chunkSize)
  }

  private def readSingleFile[F[_], T](
      inputFile: InputFile,
      filter: Filter,
      projectedSchemaResolverOpt: Option[ParquetSchemaResolver[T]],
      columnProjections: Seq[ColumnProjection],
      vcc: ValueCodecConfiguration,
      chunkSize: Int,
      metadataReader: MetadataReader,
      options: ParquetReader.Options
  )(implicit F: Sync[F]): Stream[F, Stream[F, RowParquetRecord]] =
    for {
      projectedSchemaOpt <- Stream.eval(
        projectedSchemaResolverOpt
          .traverse(implicit resolver => F.catchNonFatal(ParquetSchemaResolver.resolveSchema[T]))
      )
      parquetIterator <- Stream.resource(
        parquetIteratorResource(
          inputFile           = inputFile,
          filter              = filter.toFilterCompat(vcc),
          projectionSchemaOpt = projectedSchemaOpt,
          columnProjections   = columnProjections,
          metadataReader      = metadataReader,
          options             = options
        )
      )
    } yield Stream.fromBlockingIterator[F](parquetIterator, chunkSize)

  private def partitionedReaderStream[F[_]](
      parquetIterator: Iterator[RowParquetRecord],
      partitionedPath: PartitionedPath,
      chunkSize: Int
  )(implicit F: Sync[F]): Stream[F, RowParquetRecord] = {
    val stream = Stream.fromBlockingIterator[F](parquetIterator, chunkSize)
    if (partitionedPath.partitions.nonEmpty) {
      stream.evalMapChunk { record =>
        partitionedPath.partitions.foldLeft(F.pure(record)) { case (f, (columnPath, value)) =>
          f.flatMap { r =>
            F.catchNonFatal(r.updated(columnPath, BinaryValue(value)))
          }
        }
      }
    } else {
      stream
    }
  }

  private def parquetIteratorResource[F[_]: Sync](
      inputFile: InputFile,
      filter: FilterCompat.Filter,
      projectionSchemaOpt: Option[MessageType],
      columnProjections: Seq[ColumnProjection],
      metadataReader: MetadataReader,
      options: ParquetReader.Options
  ): Resource[F, Iterator[RowParquetRecord]] =
    Resource.fromAutoCloseable(
      Sync[F].blocking(
        new InternalParquetIterator(
          inputFile          = inputFile,
          filter             = filter,
          projectedSchemaOpt = projectionSchemaOpt,
          columnProjections  = columnProjections,
          metadataReader     = metadataReader,
          readerOptions      = options
        )
      )
    )

}
