package com.github.mjakubowski84.parquet4s.lucene

import com.github.mjakubowski84.parquet4s.*
import org.apache.lucene.analysis.standard.StandardAnalyzer
import org.apache.lucene.document.*
import org.apache.lucene.index.{IndexWriter, IndexWriterConfig}
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName
import org.apache.parquet.schema.Type
import org.apache.solr.hdfs.store.HdfsDirectory

import scala.util.Try

object Indexer {

  def apply(readerOptions: ParquetReader.Options = ParquetReader.Options()): Indexer =
    new IndexerImpl(readerOptions)

}

trait Indexer {
  def index(path: Path, col: TypedColumnPath[?], cols: TypedColumnPath[?]*): Unit
}

private class IndexerImpl(readerOptions: ParquetReader.Options) extends Indexer {
  private val configuration = readerOptions.hadoopConf
  private val analyzer      = new StandardAnalyzer() // TODO configurable
  private val indexerConfig = new IndexWriterConfig(analyzer)

  override def index(path: Path, col: TypedColumnPath[?], cols: TypedColumnPath[?]*): Unit = {
    // TODO path shall be a single file ???
    // TODO version which points a directory shall have a index-by col ???
    val indexPath = path.parent
      .getOrElse(throw new IllegalArgumentException("Invalid path"))
      .append("lucene_index")

    val iterable = ParquetReader.projectedGeneric(col, cols*).options(readerOptions).read(path)

    val index  = new HdfsDirectory(indexPath.hadoopPath, configuration)
    val writer = new IndexWriter(index, indexerConfig)
    val schema = (col +: cols).map(_.toType)

    var idx = 0L
    try iterable.foreach { record =>
      writer.addDocument(recordToDoc(path, idx, record, schema))
      idx = idx + 1L
    } finally {
      Try(writer.close())
      Try(iterable.close())
    }
  }

  private def recordToDoc(path: Path, idx: Long, record: RowParquetRecord, schema: Seq[Type]): Document = {
    // TODO we need proper schema def and converters
    val doc = schema.foldLeft(new Document()) { case (document, typ) =>
      if (typ.asPrimitiveType().getPrimitiveTypeName == PrimitiveTypeName.INT32) {
        val value = record.get(typ.getName).get.asInstanceOf[IntValue]
        document.add(new IntPoint(typ.getName, value.value))
      } else if (typ.asPrimitiveType().getPrimitiveTypeName == PrimitiveTypeName.INT64) {
        val value = record.get(typ.getName).get.asInstanceOf[LongValue]
        document.add(new LongPoint(typ.getName, value.value))
      } else if (typ.getLogicalTypeAnnotation == LogicalTypes.StringType) {
        val value = record.get(typ.getName).get
        if (value != NullValue) {
          document.add(
            new TextField(typ.getName, value.asInstanceOf[BinaryValue].value.toStringUsingUTF8, Field.Store.NO)
          )
        }
      }
      document
    }
    doc.add(new StoredField("parquet4s_file_name", path.toString))
    doc.add(new StoredField("parquet4s_record_idx", idx))
    doc
  }

}
