package com.github.mjakubowski84.parquet4s

import cats.effect.{Blocker, ContextShift, Sync}
import fs2.{Pipe, Stream}

import scala.language.higherKinds

package object parquet {

  /** Creates a [[fs2.Stream]] that reads Parquet data from the specified path. If there are multiple files at path then
    * the order in which files are loaded is determined by underlying filesystem. <br/> Path can refer to local file,
    * HDFS, AWS S3, Google Storage, Azure, etc. Please refer to Hadoop client documentation or your data provider in
    * order to know how to configure the connection. <br/> Can read also <b>partitioned</b> directories. Filter applies
    * also to partition values. Partition values are set as fields in read entities at path defined by partition name.
    * Path can be a simple column name or a dot-separated path to nested field. Missing intermediate fields are
    * automatically created for each read record. <br/> <br/>
    *
    * @param blocker
    *   used to perform blocking operations
    * @param path
    *   URI to Parquet files, e.g.: {{{"file:///data/users"}}}
    * @param options
    *   configuration of how Parquet files should be read
    * @param filter
    *   optional before-read filter; no filtering is applied by default; check [[Filter]] for more details
    * @tparam F
    *   effect type
    * @tparam T
    *   type of data that represent the schema of the Parquet data, e.g.:
    *   {{{case class MyData(id: Long, name: String, created: java.sql.Timestamp)}}}
    * @return
    *   The stream of Parquet data
    */
  @deprecated(message = "Use fromParquet instead", since = "1.6.0")
  def read[F[_]: Sync: ContextShift, T: ParquetRecordDecoder](
      blocker: Blocker,
      path: String,
      options: ParquetReader.Options = ParquetReader.Options(),
      filter: Filter                 = Filter.noopFilter
  ): Stream[F, T] =
    reader.read(blocker, path, options, filter, None)

  /** Creates a [[fs2.Stream]] that reads Parquet data from the specified path. If there are multiple files at path then
    * the order in which files are loaded is determined by underlying filesystem. <br/> Path can refer to local file,
    * HDFS, AWS S3, Google Storage, Azure, etc. Please refer to Hadoop client documentation or your data provider in
    * order to know how to configure the connection. <br/> Can read also <b>partitioned</b> directories. Filter applies
    * also to partition values. Partition values are set as fields in read entities at path defined by partition name.
    * Path can be a simple column name or a dot-separated path to nested field. Missing intermediate fields are
    * automatically created for each read record. <br/> Allows to turn on a <b>projection</b> over original file schema
    * in order to boost read performance if not all columns are required to be read.
    * @tparam F
    *   effect type
    * @tparam T
    *   type of data that represent the schema of the Parquet data, e.g.:
    *   {{{case class MyData(id: Long, name: String, created: java.sql.Timestamp)}}}
    * @return
    *   Builder of the [[fs2.Stream]]
    */
  def fromParquet[F[_], T]: reader.Builder[F, T] = reader.Builder()

  /** Creates a [[fs2.Pipe]] that writes Parquet data to single file at the specified path (including file name). The
    * resulting stream returns <b>nothing</b>, that is, it doesn't emit any element. <br/> Path can refer to local file,
    * HDFS, AWS S3, Google Storage, Azure, etc. Please refer to Hadoop client documentation or your data provider in
    * order to know how to configure the connection.
    *
    * @param path
    *   URI to Parquet files, e.g.: {{{"file:///data/users/users-2019-01-01.parquet"}}}
    * @param options
    *   set of options that define how Parquet files will be created
    * @tparam F
    *   effect type
    * @tparam T
    *   type of data that represent the schema of the Parquet data, e.g.:
    *   {{{case class MyData(id: Long, name: String, created: java.sql.Timestamp)}}}
    * @return
    *   The pipe that writes Parquet file
    */
  def writeSingleFile[F[_]: Sync: ContextShift, T: ParquetRecordEncoder: ParquetSchemaResolver](
      blocker: Blocker,
      path: String,
      options: ParquetWriter.Options = ParquetWriter.Options()
  ): Pipe[F, T, fs2.INothing] =
    writer.write(blocker, path, options)

  /** Builds a [[fs2.Pipe]] that: <ol> <li>Is designed to write Parquet files indefinitely</li> <li>Is able to
    * (optionally) partition data by a list of provided fields</li> <li>Flushes and rotates files after given number of
    * rows is written or given time period elapses</li> <li>Outputs incoming message after it is written but can write
    * an effect of provided message transformation.</li> </ol>
    *
    * @tparam F
    *   effect type
    * @tparam T
    *   type of data that represent the schema of the Parquet data, e.g.:
    *   {{{case class MyData(id: Long, name: String, created: java.sql.Timestamp)}}}
    * @return
    *   Pipe builder
    */
  def viaParquet[F[_], T]: rotatingWriter.Builder[F, T, T] = rotatingWriter.Builder()

}
