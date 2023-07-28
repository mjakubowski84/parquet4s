package com.github.mjakubowski84.parquet4s

import cats.effect.{Async, Sync}

package object parquet {

  /** Creates a [[fs2.Stream]] that reads Parquet data from the specified path. If there are multiple files at path then
    * the order in which files are loaded is determined by underlying filesystem. <br/> Path can refer to local file,
    * HDFS, AWS S3, Google Storage, Azure, etc. Please refer to Hadoop client documentation or your data provider in
    * order to know how to configure the connection. <br/> Can read also <b>partitioned</b> directories. Filter applies
    * also to partition values. Partition values are set as fields in read entities at path defined by partition name.
    * Path can be a simple column name or a dot-separated path to nested field. Missing intermediate fields are
    * automatically created for each read record. <br/> Allows to turn on a <b>projection</b> over original file schema
    * in order to boost read performance if not all columns are required to be read. <br/> Builder allows to create a
    * stream of data of given type or of generic records.
    * @tparam F
    *   effect type
    * @return
    *   Builder of the [[fs2.Stream]]
    */
  def fromParquet[F[_]: Sync]: reader.FromParquet[F] = new reader.FromParquetImpl[F]

  /** Builds a [[fs2.Pipe]] that writes Parquet data to single file at the specified path (including file name). The
    * resulting stream returns <b>nothing</b>, that is, it doesn't emit any element. <br/> Path can refer to local file,
    * HDFS, AWS S3, Google Storage, Azure, etc. Please refer to Hadoop client documentation or your data provider in
    * order to know how to configure the connection. <br/> Builder allows to create a pipe for given data type or for
    * generic records.
    * @tparam F
    *   effect type
    * @return
    *   [[fs2.Pipe]] builder
    */
  def writeSingleFile[F[_]: Sync] = new writer.ToParquetImpl[F]

  /** Builds a [[fs2.Pipe]] that: <ol> <li>Is designed to write Parquet files indefinitely</li> <li>Is able to
    * (optionally) partition data by a list of provided fields</li> <li>Flushes and rotates files after given number of
    * rows is written to the partition or a given time period elapses</li> <li>Outputs incoming message after it is
    * written but can write an effect of provided message transformation.</li> </ol> <br/> Builder allows to create a
    * pipe for given data type or for generic records.
    * @tparam F
    *   effect type
    * @return
    *   [[fs2.Pipe]] builder
    */
  def viaParquet[F[_]: Async]: rotatingWriter.ViaParquet[F] = new rotatingWriter.ViaParquetImpl[F]

}
