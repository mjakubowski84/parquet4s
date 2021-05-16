package com.github.mjakubowski84.parquet4s

import akka.Done
import akka.stream.scaladsl.Sink

import scala.concurrent.Future

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
   * Allows to turn on a <b>projection</b> over original file schema in order to boost read performance if not all
   * columns are required to be read.
   *
   * @tparam T type of data that represent the schema of the Parquet data, e.g.:
   *           {{{ case class MyData(id: Long, name: String, created: java.sql.Timestamp) }}}
   * @return Builder of the source.
   */
  def fromParquet[T]: ParquetSource.Builder[T] = ParquetSource[T]

  /**
    * Creates a [[akka.stream.scaladsl.Sink]] that writes Parquet data to single file at the specified path (including
    * file name).
    * <br/>
    * Path can refer to local file, HDFS, AWS S3, Google Storage, Azure, etc.
    * Please refer to Hadoop client documentation or your data provider in order to know how to configure the connection.
    *
    * @param path [[Path]] to Parquet files, e.g.: {{{ "file:///data/users/users-2019-01-01.parquet" }}}
    * @param options set of options that define how Parquet files will be created
    * @tparam T type of data that represent the schema of the Parquet data, e.g.:
    *           {{{ case class MyData(id: Long, name: String, created: java.sql.Timestamp) }}}
    * @return The sink that writes Parquet file
    */
  def toParquetSingleFile[T : ParquetRecordEncoder : ParquetSchemaResolver](path: Path,
                                                                            options: ParquetWriter.Options = ParquetWriter.Options()
                                                                           ): Sink[T, Future[Done]] =
    SingleFileParquetSink(path, options)

  /**
   * Builds a flow that:
   * <ol>
   *   <li>Is designed to write Parquet files indefinitely</li>
   *   <li>Is able to (optionally) partition data by a list of provided fields</li>
   *   <li>Flushes and rotates files after given number of rows is written or given time period elapses</li>
   *   <li>Outputs incoming message after it is written but can write an effect of provided message transformation.</li>
   * </ol>
   * @param path [[Path]] to Parquet files, e.g.: {{{ Path("file:///data/users") }}}
   * @tparam T type of message that flow is meant to accept
   * @return Builder of the flow.
   */
  def viaParquet[T](path: Path): ParquetPartitioningFlow.Builder[T, T] =
    ParquetPartitioningFlow.builder(path)
}
