package com.github.mjakubowski84.parquet4s

import akka.stream.scaladsl.{Sink, Source}
import akka.{Done, NotUsed}
import com.github.mjakubowski84.parquet4s.ParquetWriter.ParquetWriterFactory
import org.apache.hadoop.fs.Path
import org.apache.parquet.hadoop.{ParquetReader => HadoopParquetReader}

import scala.concurrent.Future
import scala.concurrent.duration.FiniteDuration

/**
  * Holds factory of Akka Streams sources and sinks that allow reading from and writing to Parquet files.
  */
object ParquetStreams {

  /**
    * Creates a [[akka.stream.scaladsl.Source]] that reads Parquet data from the specified path.
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
    * <b>Take note!</b> that due to an issue with implicit resolution in <b>Scala 2.11</b> you may need to define
    * <b>all parameters</b> of `ParquetStreams.fromParquet` even if some have default values. It specifically refers to
    * a case when you would like to omit `options` but define `filter`. Such situation doesn't appear in Scala 2.12 and
    * 2.13.
    *
    * @param path URI to Parquet files, e.g.: {{{ "file:///data/users" }}}
    * @param options configuration of how Parquet files should be read
    * @param filter optional before-read filter; no filtering is applied by default; check [[Filter]] for more details
    * @tparam T type of data that represent the schema of the Parquet data, e.g.:
    *           {{{ case class MyData(id: Long, name: String, created: java.sql.Timestamp) }}}
    * @return The source of Parquet data
    */
  def fromParquet[T : ParquetRecordDecoder](
                                             path: String,
                                             options: ParquetReader.Options = ParquetReader.Options(),
                                             filter: Filter = Filter.noopFilter
                                           ): Source[T, NotUsed] =
    ParquetSource(new Path(path), options, filter)

  /**
    * Creates a [[akka.stream.scaladsl.Sink]] that writes Parquet data to single file at the specified path (including
    * file name).
    * <br/>
    * Path can refer to local file, HDFS, AWS S3, Google Storage, Azure, etc.
    * Please refer to Hadoop client documentation or your data provider in order to know how to configure the connection.
    *
    * @param path URI to Parquet files, e.g.: {{{ "file:///data/users/users-2019-01-01.parquet" }}}
    * @param options set of options that define how Parquet files will be created
    * @tparam T type of data that represent the schema of the Parquet data, e.g.:
    *           {{{ case class MyData(id: Long, name: String, created: java.sql.Timestamp) }}}
    * @return The sink that writes Parquet file
    */
  def toParquetSingleFile[T : ParquetRecordEncoder : ParquetSchemaResolver](path: String,
                                                                            options: ParquetWriter.Options = ParquetWriter.Options()
                                                                           ): Sink[T, Future[Done]] =
    SingleFileParquetSink(new Path(path), options)

  /**
    * Creates a [[akka.stream.scaladsl.Sink]] that writes Parquet data to files at the specified path. Sink splits files
    * sequentially into pieces. Each file contains maximal number of records according to <i>maxRecordsPerFile</i>.
    * It is recommended to define <i>maxRecordsPerFile</i> as a multiple of
    * [[com.github.mjakubowski84.parquet4s.ParquetWriter.Options.rowGroupSize]].
    *
    * <br/>
    * Path can refer to local file, HDFS, AWS S3, Google Storage, Azure, etc.
    * Please refer to Hadoop client documentation or your data provider in order to know how to configure the connection.
    *
    * @deprecated In the future only [[viaParquet]] and [[toParquetSingleFile]] may be only supported writers
    * @param path URI to Parquet files, e.g.: {{{ "file:///data/users" }}}
    * @param maxRecordsPerFile the maximum size of file
    * @param options set of options that define how Parquet files will be created
    * @tparam T type of data that represent the schema of the Parquet data, e.g.:
    *           {{{ case class MyData(id: Long, name: String, created: java.sql.Timestamp) }}}
    * @return The sink that writes Parquet files
    */
  def toParquetSequentialWithFileSplit[T: ParquetRecordEncoder : ParquetSchemaResolver](path: String,
                                                                                        maxRecordsPerFile: Long,
                                                                                        options: ParquetWriter.Options = ParquetWriter.Options()
                                                                                       ): Sink[T, Future[Done]] =
    SequentialFileSplittingParquetSink(new Path(path), maxRecordsPerFile, options)

  /**
    * Creates a [[akka.stream.scaladsl.Sink]] that writes Parquet data to files at the specified path. Sink splits files
    * into number of pieces equal to <i>parallelism</i>. Files are written in parallel. Data is written in unordered way.
    *
    * <br/>
    * Path can refer to local file, HDFS, AWS S3, Google Storage, Azure, etc.
    * Please refer to Hadoop client documentation or your data provider in order to know how to configure the connection.
    *
    * @deprecated In the future only [[viaParquet]] and [[toParquetSingleFile]] may be only supported writers
    * @param path URI to Parquet files, e.g.: {{{ "file:///data/users" }}}
    * @param parallelism defines how many files are created and how many parallel threads are responsible for it
    * @param options set of options that define how Parquet files will be created
    * @tparam T type of data that represent the schema of the Parquet data, e.g.:
    *           {{{ case class MyData(id: Long, name: String, created: java.sql.Timestamp) }}}
    * @return The sink that writes Parquet files
    */
  def toParquetParallelUnordered[T: ParquetRecordEncoder : ParquetSchemaResolver](path: String,
                                                                                  parallelism: Int,
                                                                                  options: ParquetWriter.Options = ParquetWriter.Options()
                                                                                 ): Sink[T, Future[Done]] =
    UnorderedParallelParquetSink(new Path(path), parallelism, options)

  /**
    * Creates a [[akka.stream.scaladsl.Sink]] that writes Parquet data to files at the specified path. Sink splits files
    * when <i>maxChunkSize</i> is reached or time equal to <i>chunkWriteTimeWindow</i> elapses.
    * Files are named and written to path according to <i>buildChunkPath</i>. By default path looks like
    * <pre>PATH/part-RANDOM_UUID.parquet</pre>.
    * Objects coming into sink can be optionally transformed using <i>preWriteTransformation</i> and later handled by means
    * of <i>postWriteSink</i> after transformed object is saved to file.
    *
    * <br/>
    * Path can refer to local file, HDFS, AWS S3, Google Storage, Azure, etc.
    * Please refer to Hadoop client documentation or your data provider in order to know how to configure the connection.
    *
    * @param path URI to Parquet files, e.g.: {{{ "file:///data/users" }}}
    * @param maxChunkSize maximum number of records that can be saved to single parquet file
    * @param chunkWriteTimeWindow maximum time that sink will wait before saving (non-empty) file
    * @param buildChunkPath factory function to define custom path for each saved file
    * @param preWriteTransformation function that transforms incoming object into data that are written to the file
    * @param postWriteSink allows to to define action to be taken after each incoming object is successfully written to the file
    * @param options set of options that define how Parquet files will be created
    * @tparam In type of incoming objects
    * @tparam ToWrite type of data that represent the schema of the Parquet data, e.g.:
    *                 {{{ case class MyData(id: Long, name: String, created: java.sql.Timestamp) }}}
    * @tparam Mat type of sink's materalized value
    * @return The sink that writes Parquet files
    */
  @deprecated(message = "Use viaParquet instead", since = "1.3.0")
  def toParquetIndefinite[In, ToWrite: ParquetWriterFactory, Mat](path: String,
                                                                  maxChunkSize: Int,
                                                                  chunkWriteTimeWindow: FiniteDuration,
                                                                  buildChunkPath: ChunkPathBuilder[In] = ChunkPathBuilder.default,
                                                                  preWriteTransformation: In => ToWrite = identity[In] _,
                                                                  postWriteSink: Sink[Seq[In], Mat] = Sink.ignore,
                                                                  options: ParquetWriter.Options = ParquetWriter.Options()
                                                                 ): Sink[In, Mat] =
    IndefiniteStreamParquetSink[In, ToWrite, Mat](new Path(path), maxChunkSize, chunkWriteTimeWindow, buildChunkPath, preWriteTransformation, postWriteSink, options)

  /**
   * Builds a flow that:
   * <ol>
   *   <li>Is designed to write Parquet files indefinitely</li>
   *   <li>Is able to (optionally) partition data by a list of provided fields</li>
   *   <li>Flushes and rotates files after given number of rows is written or given time period elapses</li>
   *   <li>Outputs incoming message after it is written but can write an effect of provided message transformation.</li>
   * </ol>
   * @param path URI to Parquet files, e.g.: {{{ "file:///data/users" }}}
   * @tparam T type of message that flow is meant to accept
   * @return Builder of [[ParquetPartitioningFlow]]
   */
  def viaParquet[T](path: String): ParquetPartitioningFlow.Builder[T, T] =
    ParquetPartitioningFlow.builder(new Path(path))
}
