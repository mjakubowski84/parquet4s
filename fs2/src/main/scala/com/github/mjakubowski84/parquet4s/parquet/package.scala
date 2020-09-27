package com.github.mjakubowski84.parquet4s

import java.util.concurrent.TimeUnit

import cats.effect.{Blocker, Concurrent, ContextShift, Sync, Timer}
import fs2.{Pipe, Stream}
import org.apache.parquet.hadoop.{ParquetWriter => HadoopParquetWriter}

import scala.concurrent.duration.FiniteDuration
import scala.language.higherKinds

package object parquet {

  val DefaultMaxCount: Long = HadoopParquetWriter.DEFAULT_BLOCK_SIZE
  val DefaultMaxDuration: FiniteDuration = FiniteDuration(1, TimeUnit.MINUTES)


  /**
   * Creates a [[fs2.Stream]] that reads Parquet data from the specified path.
   * If there are multiple files at path then the order in which files are loaded is determined by underlying
   * filesystem.
   * <br/>
   * Path can refer to local file, HDFS, AWS S3, Google Storage, Azure, etc.
   * Please refer to Hadoop client documentation or your data provider in order to know how to configure the connection.
   * <br/>
   * Can read also <b>partitioned</b> directories. Filter applies also to partition values. Partition values are set
   * as fields in read entities at path defined by partition name. Path can be a simple column name or a dot-separated
   * path to nested field. Missing intermediate fields are automatically created for each read record.
   * <br/>
   * <br/>
   *
   * @param blocker used to perform blocking operations
   * @param path URI to Parquet files, e.g.: {{{ "file:///data/users" }}}
   * @param options configuration of how Parquet files should be read
   * @param filter optional before-read filter; no filtering is applied by default; check [[Filter]] for more details
   * @tparam F effect type
   * @tparam T type of data that represent the schema of the Parquet data, e.g.:
   *           {{{ case class MyData(id: Long, name: String, created: java.sql.Timestamp) }}}
   * @return The stream of Parquet data
   */
  def read[F[_]: Sync: ContextShift, T: ParquetRecordDecoder](blocker: Blocker,
                                                              path: String,
                                                              options: ParquetReader.Options = ParquetReader.Options(),
                                                              filter: Filter = Filter.noopFilter
                                                             ): Stream[F, T] =
    reader.read(blocker, path, options, filter)


  /**
   * Creates a [[fs2.Pipe]] that writes Parquet data to single file at the specified path (including
   * file name).
   * <br/>
   * Path can refer to local file, HDFS, AWS S3, Google Storage, Azure, etc.
   * Please refer to Hadoop client documentation or your data provider in order to know how to configure the connection.
   *
   * @param path URI to Parquet files, e.g.: {{{ "file:///data/users/users-2019-01-01.parquet" }}}
   * @param options set of options that define how Parquet files will be created
   * @tparam F effect type
   * @tparam T type of data that represent the schema of the Parquet data, e.g.:
   *           {{{ case class MyData(id: Long, name: String, created: java.sql.Timestamp) }}}
   * @return The pipe that writes Parquet file
   */
  def writeSingleFile[F[_]: Sync: ContextShift, T : ParquetRecordEncoder : ParquetSchemaResolver](blocker: Blocker,
                                                                                                  path: String,
                                                                                                  options: ParquetWriter.Options = ParquetWriter.Options()
                                                                                                 ): Pipe[F, T, Unit] =
    writer.write(blocker, path, options)

  /**
   * Builds a [[fs2.Pipe]] that:
   * <ol>
   *   <li>Is designed to write Parquet files indefinitely</li>
   *   <li>Is able to (optionally) partition data by a list of provided fields</li>
   *   <li>Flushes and rotates files after given number of rows is written or given time period elapses</li>
   *   <li>Outputs incoming message after it is written but can write an effect of provided message transformation.</li>
   * </ol>
   *
   * @tparam F effect type
   * @tparam T type of data that represent the schema of the Parquet data, e.g.:
   *           {{{ case class MyData(id: Long, name: String, created: java.sql.Timestamp) }}}
   * @return
   */
  def viaParquet[F[_], T]: Builder[F, T, T] =
    BuilderImpl[F, T, T](
      maxCount = DefaultMaxCount,
      maxDuration = DefaultMaxDuration,
      preWriteTransformation = t => Stream.emit(t),
      partitionBy = Seq.empty,
      writeOptions = ParquetWriter.Options()
    )


  trait Builder[F[_], T, W] {
    /**
     * @param maxCount max number of records to be written before file rotation
     */
    def maxCount(maxCount: Long): Builder[F, T, W]
    /**
     * @param maxDuration max time after which partition file is rotated
     */
    def maxDuration(maxDuration: FiniteDuration): Builder[F, T, W]
    /**
     * @param writeOptions writer options used by the flow
     */
    def options(writeOptions: ParquetWriter.Options): Builder[F, T, W]
    /**
     * Sets partition paths that stream partitions data by. Can be empty.
     * Partition path can be a simple string column (e.g. "color") or a dot-separated path pointing nested string field
     * (e.g. "user.address.postcode"). Partition path is used to extract data from the entity and to create
     * a tree of subdirectories for partitioned files. Using aforementioned partitions effects in creation
     * of (example) following tree:
     * {{{
     * ../color=blue
     *      /user.address.postcode=XY1234/
     *      /user.address.postcode=AB4321/
     *   /color=green
     *      /user.address.postcode=XY1234/
     *      /user.address.postcode=CV3344/
     *      /user.address.postcode=GH6732/
     * }}}
     * Take <b>note</b>:
     * <ol>
     *   <li>PartitionBy must point a string field.</li>
     *   <li>Partitioning removes partition fields from the schema. Data is stored in name of subdirectory
     *       instead of Parquet file.</li>
     *   <li>Partitioning cannot end in having empty schema. If you remove all fields of the message you will
     *       get an error.</li>
     *   <li>Partitioned directories can be filtered effectively during reading.</li>
     * </ol>
     * @param partitionBy partition paths
     */
    def partitionBy(partitionBy: String*): Builder[F, T, W]
    /**
     * @param transformation function that is called by stream in order to obtain Parquet schema. Identity by default.
     * @tparam X Schema type
     */
    def withPreWriteTransformation[X](transformation: T => Stream[F, X]): Builder[F, T, X]
    /**
     * Builds final writer pipe.
     */
    def write(blocker: Blocker, basePath: String)(implicit
                                                  schemaResolver: SkippingParquetSchemaResolver[W],
                                                  encoder: ParquetRecordEncoder[W],
                                                  sync: Sync[F],
                                                  timer : Timer[F],
                                                  concurrent: Concurrent[F],
                                                  contextShift: ContextShift[F]): Pipe[F, T, T]
  }

  private case class BuilderImpl[F[_], T, W](
                                             maxCount: Long,
                                             maxDuration: FiniteDuration,
                                             preWriteTransformation: T => Stream[F, W],
                                             partitionBy: Seq[String],
                                             writeOptions: ParquetWriter.Options
                                            ) extends Builder[F, T, W] {

    override def maxCount(maxCount: Long): Builder[F, T, W] = copy(maxCount = maxCount)
    override def maxDuration(maxDuration: FiniteDuration): Builder[F, T, W] = copy(maxDuration = maxDuration)
    override def options(writeOptions: ParquetWriter.Options): Builder[F, T, W] = copy(writeOptions = writeOptions)
    override def partitionBy(partitionBy: String*): Builder[F, T, W] = copy(partitionBy = partitionBy)
    override def write(blocker: Blocker, basePath: String)(implicit
                                                           schemaResolver: SkippingParquetSchemaResolver[W],
                                                           encoder: ParquetRecordEncoder[W],
                                                           sync: Sync[F],
                                                           timer : Timer[F],
                                                           concurrent: Concurrent[F],
                                                           contextShift: ContextShift[F]): Pipe[F, T, T] =
      rotatingWriter.write[F, T, W](blocker, basePath, maxCount, maxDuration, partitionBy, preWriteTransformation ,writeOptions)

    override def withPreWriteTransformation[X](transformation: T => Stream[F, X]): Builder[F, T, X] =
      BuilderImpl(
        maxCount = maxCount,
        maxDuration = maxDuration,
        preWriteTransformation = transformation,
        partitionBy = partitionBy,
        writeOptions = writeOptions
      )
  }
}
