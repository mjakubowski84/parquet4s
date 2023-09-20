package com.github.mjakubowski84.parquet4s

import akka.NotUsed
import akka.stream.scaladsl.Source
import org.apache.parquet.filter2.compat.FilterCompat
import org.apache.parquet.hadoop.util.HadoopInputFile
import org.apache.parquet.io.InputFile
import org.apache.parquet.schema.{MessageType, Type}
import org.slf4j.{Logger, LoggerFactory}

object ParquetSource extends IOOps {

  /** Factory of builders of Parquet readers.
    */
  trait FromParquet {

    /** Creates [[Builder]] of Parquet reader for documents of type <i>T</i>.
      */
    def as[T: ParquetRecordDecoder]: Builder[T]

    /** Creates [[Builder]] of Parquet reader for <i>projected</i> documents of type <i>T</i>. Due to projection reader
      * does not attempt to read all existing columns of the file but applies enforced projection schema.
      */
    def projectedAs[T: ParquetRecordDecoder: ParquetSchemaResolver]: Builder[T]

    /** Creates [[Builder]] of Parquet reader of generic records.
      */
    def generic: Builder[RowParquetRecord]

    /** Creates [[Builder]] of Parquet reader of <i>projected</i> generic records. Due to projection reader does not
      * attempt to read all existing columns of the file but applies enforced projection schema.
      */
    def projectedGeneric(projectedSchema: MessageType): Builder[RowParquetRecord]

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
    def projectedGeneric(col: TypedColumnPath[?], cols: TypedColumnPath[?]*): Builder[RowParquetRecord]

    /** Creates [[CustomBuilder]] of Parquet reader data using custom internal implementation.
      */
    @experimental
    def custom[T](builder: org.apache.parquet.hadoop.ParquetReader.Builder[T]): CustomBuilder[T]
  }

  private[parquet4s] object FromParquetImpl extends FromParquet {
    override def as[T: ParquetRecordDecoder]: Builder[T] = BuilderImpl()
    override def projectedAs[T: ParquetRecordDecoder: ParquetSchemaResolver]: Builder[T] = BuilderImpl(
      projectedSchemaResolverOpt = Option(implicitly[ParquetSchemaResolver[T]])
    )
    override def generic: Builder[RowParquetRecord] = BuilderImpl()
    override def projectedGeneric(projectedSchema: MessageType): Builder[RowParquetRecord] =
      BuilderImpl[RowParquetRecord](
        projectedSchemaResolverOpt = Option(RowParquetRecord.genericParquetSchemaResolver(projectedSchema))
      )
    override def projectedGeneric(col: TypedColumnPath[?], cols: TypedColumnPath[?]*): Builder[RowParquetRecord] = {
      val (fields, columnProjections) =
        (col +: cols.toVector).zipWithIndex
          .foldLeft((Vector.empty[Type], Vector.empty[ColumnProjection])) {
            case ((fields, projections), (columnPath, ordinal)) =>
              val updatedFields      = fields :+ columnPath.toType
              val updatedProjections = projections :+ ColumnProjection(columnPath, ordinal)
              updatedFields -> updatedProjections
          }
      BuilderImpl(
        projectedSchemaResolverOpt = Option(RowParquetRecord.genericParquetSchemaResolver(Message.merge(fields))),
        columnProjections          = columnProjections
      )
    }

    override def custom[T](builder: org.apache.parquet.hadoop.ParquetReader.Builder[T]): CustomBuilder[T] =
      CustomBuilderImpl(
        builder = builder,
        options = ParquetReader.Options(),
        filter  = Filter.noopFilter
      )
  }

  /** Builds instance of Parquet [[akka.stream.scaladsl.Source]]
    * @tparam T
    *   type of data generated by the source.
    */
  trait Builder[T] {

    /** @param options
      *   configuration of how Parquet files should be read
      */
    def options(options: ParquetReader.Options): Builder[T]

    /** @param filter
      *   optional before-read filter; no filtering is applied by default; check [[Filter]] for more details
      */
    def filter(filter: Filter): Builder[T]

    /** @param path
      *   [[Path]] to Parquet files, e.g.: {{{Path("file:///data/users")}}}
      * @return
      *   final [[akka.stream.scaladsl.Source]]
      */
    def read(path: Path): Source[T, NotUsed]

    /** @param inputFile
      *   file to read
      * @return
      *   final [[akka.stream.scaladsl.Source]]
      */
    def read(inputFile: InputFile): Source[T, NotUsed]
  }

  private case class BuilderImpl[T: ParquetRecordDecoder](
      options: ParquetReader.Options                               = ParquetReader.Options(),
      filter: Filter                                               = Filter.noopFilter,
      projectedSchemaResolverOpt: Option[ParquetSchemaResolver[T]] = None,
      columnProjections: Seq[ColumnProjection]                     = Seq.empty
  ) extends Builder[T] {
    override def options(options: ParquetReader.Options): Builder[T] =
      this.copy(options = options)

    override def filter(filter: Filter): Builder[T] =
      this.copy(filter = filter)

    override def read(path: Path): Source[T, NotUsed] =
      read(path.toInputFile(options))

    override def read(inputFile: InputFile): Source[T, NotUsed] =
      ParquetSource(inputFile, options, filter, projectedSchemaResolverOpt, columnProjections)

  }

  @experimental
  trait CustomBuilder[T] {

    /** @param options
      *   configuration of how Parquet files should be read
      */
    def options(options: ParquetReader.Options): CustomBuilder[T]

    /** @param filter
      *   optional before-read filter; no filtering is applied by default; check [[Filter]] for more details
      */
    def filter(filter: Filter): CustomBuilder[T]

    /** @param readMap
      *   called on each element immediately after it is read
      * @return
      *   final [[akka.stream.scaladsl.Source]]
      */
    def read[X](readMap: T => X = identity _): Source[X, NotUsed]

  }

  private case class CustomBuilderImpl[T](
      builder: org.apache.parquet.hadoop.ParquetReader.Builder[T],
      options: ParquetReader.Options,
      filter: Filter
  ) extends CustomBuilder[T] {

    /** @param options
      *   configuration of how Parquet files should be read
      */
    def options(options: ParquetReader.Options): CustomBuilder[T] = this.copy(options = options)

    /** @param filter
      *   optional before-read filter; no filtering is applied by default; check [[Filter]] for more details
      */
    def filter(filter: Filter): CustomBuilder[T] = this.copy(filter = filter)

    /** @return
      *   final [[akka.stream.scaladsl.Source]]
      */
    def read[X](readMap: T => X = identity _): Source[X, NotUsed] = {
      val filterCompat = filter.toFilterCompat(ValueCodecConfiguration(options))
      Source
        .unfoldResource[X, org.apache.parquet.hadoop.ParquetReader[T]](
          create = () => builder.withConf(options.hadoopConf).withFilter(filterCompat).build(),
          read   = reader => Option(reader.read()).map(readMap),
          close  = _.close()
        )
    }

  }

  override protected val logger: Logger = LoggerFactory.getLogger(this.getClass)

  private def apply[T](
      inputFile: InputFile,
      options: ParquetReader.Options,
      filter: Filter,
      projectedSchemaResolverOpt: Option[ParquetSchemaResolver[T]],
      columnProjections: Seq[ColumnProjection]
  )(implicit decoder: ParquetRecordDecoder[T]): Source[T, NotUsed] = {
    val valueCodecConfiguration = ValueCodecConfiguration(options)
    val hadoopConf              = options.hadoopConf

    def decode(record: RowParquetRecord): T = ParquetRecordDecoder.decode[T](record, valueCodecConfiguration)

    val recordSource = inputFile match {
      case hadoopInputFile: HadoopInputFile =>
        findPartitionedPaths(Path(hadoopInputFile.getPath), hadoopConf).fold(
          Source.failed,
          partitionedDirectory => {
            val projectedSchemaOpt = projectedSchemaResolverOpt
              .map(implicit resolver => ParquetSchemaResolver.resolveSchema(partitionedDirectory.schema))
            val sources = PartitionFilter
              .filter(filter, valueCodecConfiguration, partitionedDirectory)
              .map(createPartitionedSource(projectedSchemaOpt, columnProjections, decoder).tupled)

            if (sources.isEmpty) Source.empty
            else sources.reduceLeft(_.concat(_))
          }
        )
      case _ =>
        val projectedSchemaOpt =
          projectedSchemaResolverOpt.map(implicit resolver => ParquetSchemaResolver.resolveSchema[T])
        createSource(
          inputFile,
          projectedSchemaOpt,
          columnProjections,
          filter.toFilterCompat(valueCodecConfiguration),
          decoder
        )
    }

    recordSource.map(decode)
  }

  private def createPartitionedSource(
      projectedSchemaOpt: Option[MessageType],
      columnProjections: Seq[ColumnProjection],
      decoder: ParquetRecordDecoder[?]
  ): (FilterCompat.Filter, PartitionedPath) => Source[RowParquetRecord, NotUsed] = { (filterCompat, partitionedPath) =>
    createSource(partitionedPath.inputFile, projectedSchemaOpt, columnProjections, filterCompat, decoder)
      .map(setPartitionValues(partitionedPath))
  }

  private def createSource(
      inputFile: InputFile,
      projectedSchemaOpt: Option[MessageType],
      columnProjections: Seq[ColumnProjection],
      filterCompat: FilterCompat.Filter,
      decoder: ParquetRecordDecoder[?]
  ) =
    Source
      .unfoldResource[RowParquetRecord, org.apache.parquet.hadoop.ParquetReader[RowParquetRecord]](
        create = () => createReader(filterCompat, inputFile, projectedSchemaOpt, columnProjections, decoder),
        read   = reader => Option(reader.read()),
        close  = _.close()
      )

  private def setPartitionValues(partitionedPath: PartitionedPath)(record: RowParquetRecord) =
    partitionedPath.partitions.foldLeft(record) { case (currentRecord, (columnPath, value)) =>
      currentRecord.updated(columnPath, BinaryValue(value))
    }

  private def createReader(
      filterCompat: FilterCompat.Filter,
      inputFile: InputFile,
      projectedSchemaOpt: Option[MessageType],
      columnProjections: Seq[ColumnProjection],
      decoder: ParquetRecordDecoder[?]
  ): org.apache.parquet.hadoop.ParquetReader[RowParquetRecord] =
    HadoopParquetReader(inputFile, projectedSchemaOpt, columnProjections, filterCompat, decoder).build()
}
