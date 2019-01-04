package com.github.mjakubowski84.parquet4s

import akka.NotUsed
import akka.stream.scaladsl.Source
import org.apache.hadoop.fs.Path
import org.apache.parquet.hadoop.{ParquetReader => HadoopParquetReader}

/**
  * Holds factory of Akka Streams source that allows reading Parquet files.
  */
object ParquetStreams {

  /**
    * Creates a source that reads Parquet data from the specified path.
    * <br/>
    * Path can refer to local file, HDFS, AWS S3, Google Storage, Azure, etc.
    * Please refer to Hadoop client documentation or your data provider in order to know how to configure the connection.
    *
    * @param path URI to Parquet files, e.g.:
    *             {{{ "file:///data/users" }}}
    * @tparam T type of data that represent the schema of the Parquet data, e.g.:
    *           {{{
    *             case class MyData(id: Long, name: String, created: java.sql.Timestamp)
    *           }}}
    * @return
    */
  def fromParquet[T : ParquetRecordDecoder](path: String): Source[T, NotUsed] =
    Source.unfoldResource[RowParquetRecord, HadoopParquetReader[RowParquetRecord]](
      create = HadoopParquetReader.builder[RowParquetRecord](new ParquetReadSupport(), new Path(path)).build,
      read = reader => Option(reader.read()),
      close = _.close()
    ).map(ParquetRecordDecoder.decode[T])

}
