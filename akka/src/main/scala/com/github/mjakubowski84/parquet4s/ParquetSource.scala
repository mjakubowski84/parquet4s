package com.github.mjakubowski84.parquet4s

import akka.NotUsed
import akka.stream.scaladsl.Source
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.parquet.filter2.compat.FilterCompat
import org.apache.parquet.hadoop.{ParquetReader => HadoopParquetReader}
import org.apache.parquet.schema.MessageType
import org.slf4j.{Logger, LoggerFactory}

object ParquetSource extends IOOps {

  /**
   * Buils instance of Parquet [[akka.stream.scaladsl.Source]]
   * @tparam T type of data generated by the source.
   */
  trait Builder[T] {
    /**
     * @param options configuration of how Parquet files should be read
     */
    def withOptions(options: ParquetReader.Options): Builder[T]
    /**
     * @param filter optional before-read filter; no filtering is applied by default; check [[Filter]] for more details
     */
    def withFilter(filter: Filter): Builder[T]
    /**
     * @param schemaResolver resolved schema that is going to be used as a projection over original file schema
     */
    def withProjection(implicit schemaResolver: ParquetSchemaResolver[T]): Builder[T]
    /**
     * @param path URI to Parquet files, e.g.: {{{ "file:///data/users" }}}
     * @param decoder decodes [[RowParquetRecord]] to your data type
     * @return final [[akka.stream.scaladsl.Source]]
     */
    def read(path: String)(implicit decoder: ParquetRecordDecoder[T]): Source[T, NotUsed]
  }

  private case class BuilderImpl[T](
                                    options: ParquetReader.Options,
                                    filter: Filter,
                                    projectedSchemaOpt: Option[MessageType]
                                   ) extends Builder[T] {
    override def withOptions(options: ParquetReader.Options): Builder[T] =
      this.copy(options = options)

    override def withFilter(filter: Filter) : Builder[T] =
      this.copy(filter = filter)

    override def withProjection(implicit schemaResolver: ParquetSchemaResolver[T]): Builder[T] =
      this.copy(projectedSchemaOpt = Option(ParquetSchemaResolver.resolveSchema[T]))

    override def read(path: String)(implicit decoder: ParquetRecordDecoder[T]): Source[T, NotUsed] =
      ParquetSource.apply(new Path(path), options, filter, projectedSchemaOpt)

  }

  override protected lazy val logger: Logger = LoggerFactory.getLogger(this.getClass)

  private[parquet4s] def apply[T]: Builder[T] =
    BuilderImpl(
      options = ParquetReader.Options(),
      filter = Filter.noopFilter,
      projectedSchemaOpt = None
    )

  private[parquet4s] def apply[T: ParquetRecordDecoder](path: Path,
                                                        options: ParquetReader.Options,
                                                        filter: Filter,
                                                        projectedSchemaOpt: Option[MessageType]
                                                       ): Source[T, NotUsed] = {
    val valueCodecConfiguration = options.toValueCodecConfiguration
    val hadoopConf = options.hadoopConf

    findPartitionedPaths(path, hadoopConf).fold(
      Source.failed,
      partitionedDirectory => {
        val sources = PartitionFilter
          .filter(filter, valueCodecConfiguration, partitionedDirectory)
          .map(createSource[T](valueCodecConfiguration, hadoopConf, projectedSchemaOpt).tupled)

        if (sources.isEmpty) Source.empty
        else sources.reduceLeft(_.concat(_))
      }
    )
  }

  private def createSource[T: ParquetRecordDecoder](valueCodecConfiguration: ValueCodecConfiguration,
                                                    hadoopConf: Configuration,
                                                    projectedSchemaOpt: Option[MessageType]
                                                   ):
                                                   (FilterCompat.Filter, PartitionedPath) => Source[T, NotUsed] = {
    (filterCompat, partitionedPath) =>
      def decode(record: RowParquetRecord): T = ParquetRecordDecoder.decode[T](record, valueCodecConfiguration)

      Source.unfoldResource[RowParquetRecord, HadoopParquetReader[RowParquetRecord]](
        create = () => createReader(hadoopConf, filterCompat, partitionedPath, projectedSchemaOpt),
        read = reader => Option(reader.read()),
        close = _.close()
      ).map { record =>
        partitionedPath.partitions.foreach { case (columnPath, value) =>
          record.add(columnPath, BinaryValue(value))
        }
        record
      }.map(decode)
  }

  private def createReader(hadoopConf: Configuration,
                           filterCompat: FilterCompat.Filter,
                           partitionedPath: PartitionedPath,
                           projectedSchemaOpt: Option[MessageType]
                          ): HadoopParquetReader[RowParquetRecord] =
    HadoopParquetReader.builder[RowParquetRecord](new ParquetReadSupport(projectedSchemaOpt), partitionedPath.path)
      .withConf(hadoopConf)
      .withFilter(filterCompat)
      .build()

}
