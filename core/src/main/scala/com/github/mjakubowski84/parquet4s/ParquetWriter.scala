package com.github.mjakubowski84.parquet4s

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.parquet.hadoop.api.WriteSupport
import org.apache.parquet.hadoop.api.WriteSupport.WriteContext
import org.apache.parquet.hadoop.{ParquetWriter => HadoopParquetWriter}
import org.apache.parquet.io.api.RecordConsumer
import org.apache.parquet.schema.MessageType

import scala.collection.JavaConverters._


trait ParquetWriter[T] {

  def write(path: String, data: Iterable[T])

}


object ParquetWriter  {

  private class Builder(path: String, schema: MessageType) extends HadoopParquetWriter.Builder[RowParquetRecord, Builder](new Path(path)) {
    println("Using schema:\n" + schema)

    override def self(): Builder = this

    override def getWriteSupport(conf: Configuration): WriteSupport[RowParquetRecord] = new ParquetWriteSupport(schema, Map.empty)
  }

  def write[T](path: String, data: Iterable[T])(implicit writer: ParquetWriter[T]): Unit = writer.write(path, data)

  implicit def writer[T: ParquetRecordEncoder : ParquetSchemaResolver.SchemaResolver]: ParquetWriter[T] = new ParquetWriter[T] {
    override def write(path: String, data: Iterable[T]): Unit = {

      val records = data.map(ParquetRecordEncoder.encode[T])

      val writer = new Builder(path, ParquetSchemaResolver.resolveSchema).build()
      records.foreach(writer.write)

      writer.close()
    }
  }

}

private class ParquetWriteSupport(schema: MessageType, metadata: Map[String, String]) extends WriteSupport[RowParquetRecord] {
  private var consumer: RecordConsumer = _

  override def init(configuration: Configuration): WriteContext = new WriteContext(schema, metadata.asJava)


  override def write(record: RowParquetRecord): Unit = {
    consumer.startMessage()
    record.getMap.foreach {
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
