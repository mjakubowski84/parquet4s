package com.github.mjakubowski84.parquet4s

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.parquet.hadoop.api.WriteSupport
import org.apache.parquet.hadoop.api.WriteSupport.WriteContext
import org.apache.parquet.hadoop.{ParquetWriter => HadoopParquetWriter}
import org.apache.parquet.io.api.RecordConsumer
import org.apache.parquet.schema.MessageType

import scala.collection.JavaConverters._


/**
  * Type class that allows to write data which scehma is represented by type <i>T</i> to given path.
  * @tparam T schema of data to write
  */
trait ParquetWriter[T] {

  /**
    * Writes data to given path.
    * @param path location where files are meant to be written
    * @param data data to write
    */
  def write(path: String, data: Iterable[T])

}


object ParquetWriter  {

  private class Builder(path: String, schema: MessageType) extends HadoopParquetWriter.Builder[RowParquetRecord, Builder](new Path(path)) {
    println("Using schema:\n" + schema)

    override def self(): Builder = this

    override def getWriteSupport(conf: Configuration): WriteSupport[RowParquetRecord] = new ParquetWriteSupport(schema, Map.empty)
  }

  /**
    * Writes iterable collection of data as a Parquet files at given path.
    * Path can represent local file or directory, HDFS, AWS S3, Google Storage, Azure, etc.
    * Please refer to Hadoop client documentation or your data provider in order to know how to configure the connection.
    *
    * @param path URI where the data will be written to
    * @param data Collection of <i>T</> that will be written in Parquet file format
    * @param writer [[ParquetWriter]] that will be used to write data
    * @tparam T type of data, will be used also to resolve the schema of Parquet files
    */
  def write[T](path: String, data: Iterable[T])(implicit writer: ParquetWriter[T]): Unit = writer.write(path, data)

  /**
    * Default instance of [[ParquetWriter]]
    */
  implicit def writer[T: ParquetRecordEncoder : ParquetSchemaResolver]: ParquetWriter[T] = new ParquetWriter[T] {
    override def write(path: String, data: Iterable[T]): Unit = {

      val records = data.map(ParquetRecordEncoder.encode[T])

      val writer = new Builder(path, ParquetSchemaResolver.resolveSchema).build()
      try {
        records.foreach(writer.write)
      } finally {
        writer.close()
      }
    }
  }

}

private class ParquetWriteSupport(schema: MessageType, metadata: Map[String, String]) extends WriteSupport[RowParquetRecord] {
  private var consumer: RecordConsumer = _

  override def init(configuration: Configuration): WriteContext = new WriteContext(schema, metadata.asJava)


  override def write(record: RowParquetRecord): Unit = {
    consumer.startMessage()
    record.fields.foreach {
      case (_, NullValue) =>
        // ignoring nulls
      case (name, value) =>
        val fieldIndex = schema.getFieldIndex(name)
        consumer.startField(name, fieldIndex)
        value.write(schema.getType(fieldIndex), consumer)
        consumer.endField(name, fieldIndex)
    }
    consumer.endMessage()
  }

  override def prepareForWrite(recordConsumer: RecordConsumer): Unit = {
    consumer = recordConsumer
  }
}
