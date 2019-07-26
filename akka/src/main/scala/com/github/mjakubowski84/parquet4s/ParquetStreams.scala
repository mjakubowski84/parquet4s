package com.github.mjakubowski84.parquet4s

import akka.stream.scaladsl.{Sink, Source}
import akka.{Done, NotUsed}
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
    *
    * @param path URI to Parquet files, e.g.: {{{ "file:///data/users" }}}
    * @tparam T type of data that represent the schema of the Parquet data, e.g.:
    *           {{{ case class MyData(id: Long, name: String, created: java.sql.Timestamp) }}}
    * @return The source of Parquet data
    */
  def fromParquet[T : ParquetRecordDecoder](path: String, options: ParquetReader.Options = ParquetReader.Options()): Source[T, NotUsed] = {
    val valueCodecConfiguration = options.toValueCodecConfiguration
    def decode(record: RowParquetRecord): T =  ParquetRecordDecoder.decode[T](record, valueCodecConfiguration)
    Source.unfoldResource[RowParquetRecord, HadoopParquetReader[RowParquetRecord]](
      create = HadoopParquetReader.builder[RowParquetRecord](new ParquetReadSupport(), new Path(path)).withConf(options.hadoopConf).build,
      read = reader => Option(reader.read()),
      close = _.close()
    ).map(decode)
  }

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
  def toParquetIndefinite[In, ToWrite: ParquetWriter, Mat](path: String,
                                                           maxChunkSize: Int,
                                                           chunkWriteTimeWindow: FiniteDuration,
                                                           buildChunkPath: ChunkPathBuilder[In] = ChunkPathBuilder.default,
                                                           preWriteTransformation: In => ToWrite = identity[In] _,
                                                           postWriteSink: Sink[Seq[In], Mat] = Sink.ignore,
                                                           options: ParquetWriter.Options = ParquetWriter.Options()
                                                          ): Sink[In, Mat] =
    IndefiniteStreamParquetSink(new Path(path), maxChunkSize, chunkWriteTimeWindow, buildChunkPath, preWriteTransformation, postWriteSink, options)
}
