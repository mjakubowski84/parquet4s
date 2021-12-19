package com.github.mjakubowski84.parquet4s

import org.apache.hadoop.conf.Configuration
import org.apache.parquet.hadoop.ParquetReader as HadoopParquetReader
import org.apache.parquet.schema.{MessageType, Type}

import java.util.TimeZone

object ParquetReader {

  /** Builds an instance of [[ParquetIterable]]
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
      * @param decoder
      *   decodes [[RowParquetRecord]] to your data type
      * @return
      *   final [[ParquetIterable]]
      */
    def read(path: Path)(implicit decoder: ParquetRecordDecoder[T]): ParquetIterable[T]
  }

  private case class BuilderImpl[T](
      options: ParquetReader.Options          = ParquetReader.Options(),
      filter: Filter                          = Filter.noopFilter,
      projectedSchemaOpt: Option[MessageType] = None,
      lookups: Seq[Lookup]                    = Seq.empty
  ) extends Builder[T] {
    override def options(options: ParquetReader.Options): Builder[T] =
      this.copy(options = options)

    override def filter(filter: Filter): Builder[T] =
      this.copy(filter = filter)

    override def read(path: Path)(implicit decoder: ParquetRecordDecoder[T]): ParquetIterable[T] = {
      val valueCodecConfiguration = ValueCodecConfiguration(options)
      ParquetIterable.apply(
        iteratorFactory = () =>
          new ParquetIterator(
            HadoopParquetReader
              .builder[RowParquetRecord](new ParquetReadSupport(projectedSchemaOpt, lookups), path.toHadoop)
              .withConf(options.hadoopConf)
              .withFilter(filter.toFilterCompat(valueCodecConfiguration))
          ),
        valueCodecConfiguration = valueCodecConfiguration,
        stats                   = Stats(path, options, projectedSchemaOpt, filter)
      )
    }
  }

  /** Configuration settings that are used during decoding or reading Parquet files
    * @param timeZone
    *   set it to [[java.util.TimeZone]] which was used to encode time-based data that you want to read; machine's time
    *   zone is used by default
    * @param hadoopConf
    *   use it to programmatically override Hadoop's [[org.apache.hadoop.conf.Configuration]]
    */
  case class Options(timeZone: TimeZone = TimeZone.getDefault, hadoopConf: Configuration = new Configuration())

  /** Creates new [[ParquetIterable]] over data from given path. <br/> Path can represent local file or directory, HDFS,
    * AWS S3, Google Storage, Azure, etc. Please refer to Hadoop client documentation or your data provider in order to
    * know how to configure the connection.
    *
    * @note
    *   Remember to call `close()` on iterable in order to free resources!
    *
    * @param path
    *   [[Path]] to Parquet files, e.g.: {{{Path("file:///data/users")}}}
    * @param options
    *   configuration of how Parquet files should be read
    * @param filter
    *   optional before-read filtering; no filtering is applied by default; check [[Filter]] for more details
    * @tparam T
    *   type of data that represents the schema of the Parquet file, e.g.:
    *   {{{case class MyData(id: Long, name: String, created: java.sql.Timestamp)}}}
    */
  @deprecated("2.0.0", "use builder API by calling 'as[T]', 'projectedAs[T]', 'generic' or 'projectedGeneric'")
  def read[T: ParquetRecordDecoder: ParquetSchemaResolver](
      path: Path,
      options: Options = Options(),
      filter: Filter   = Filter.noopFilter
  ): ParquetIterable[T] =
    projectedAs[T].options(options).filter(filter).read(path)

  /** Creates [[Builder]] of Parquet reader for documents of type <i>T</i>.
    */
  def as[T]: Builder[T] = BuilderImpl()

  /** Creates [[Builder]] of Parquet reader for <i>projected</i> documents of type <i>T</i>. Due to projection reader
    * does not attempt to read all existing columns of the file but applies enforced projection schema.
    */
  def projectedAs[T: ParquetSchemaResolver]: Builder[T] = BuilderImpl(
    projectedSchemaOpt = Option(ParquetSchemaResolver.resolveSchema[T])
  )

  /** Creates [[Builder]] of Parquet reader returning generic records.
    */
  def generic: Builder[RowParquetRecord] = BuilderImpl()

  /** Creates [[Builder]] of Parquet reader returning <i>projected</i> generic records. Due to projection reader does
    * not attempt to read all existing columns of the file but applies enforced projection schema.
    */
  def projectedGeneric(projectedSchema: MessageType): Builder[RowParquetRecord] = BuilderImpl(
    projectedSchemaOpt = Option(projectedSchema)
  )

  /** TODO docs
    * @param col
    *   first column projection
    * @param cols
    *   next column projections
    */
  def projectedGeneric(col: TypedColumnPath[?], cols: TypedColumnPath[?]*): Builder[RowParquetRecord] = {
    val (fields, lookups) =
      (col +: cols.toList).zipWithIndex
        .foldLeft((Vector.empty[Type], Vector.empty[Lookup])) { case ((fields, lookups), (columnPath, ordinal)) =>
          val updatedFields  = fields :+ columnPath.toType
          val updatedLookups = lookups :+ Lookup(columnPath, ordinal)
          updatedFields -> updatedLookups
        }
    BuilderImpl(
      projectedSchemaOpt = Option(Message.merge(fields)),
      lookups            = lookups
    )
  }

}
