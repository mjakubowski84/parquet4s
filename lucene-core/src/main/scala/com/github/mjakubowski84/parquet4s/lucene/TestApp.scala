package com.github.mjakubowski84.parquet4s.lucene

import com.github.mjakubowski84.parquet4s.Path
import org.apache.hadoop.conf.Configuration
import org.apache.lucene.analysis.standard.StandardAnalyzer
import org.apache.lucene.document.{Document, Field, StringField, TextField}
import org.apache.lucene.index.{DirectoryReader, IndexReader, IndexWriter, IndexWriterConfig}
import org.apache.lucene.queryparser.classic.QueryParser
import org.apache.lucene.search.IndexSearcher
import org.apache.lucene.store.Directory
import org.apache.solr.hdfs.store.HdfsDirectory

object TestApp extends App {

  val analyzer      = new StandardAnalyzer()
  val configuration = new Configuration()
  val index: Directory = new HdfsDirectory(
    Path("/Users/mjakubowski/Downloads/lucene").hadoopPath,
    configuration
  )

  read()

  def write() = {
    val config = new IndexWriterConfig(analyzer)
    val writer = new IndexWriter(index, config)

    addDoc(writer, "Lucene in Action", "193398817")
    addDoc(writer, "Lucene for Dummies", "55320055Z")
    addDoc(writer, "Managing Gigabytes", "55063554A")
    addDoc(writer, "The Art of Computer Science", "9900333X")
    writer.close()
  }

  def read() = {
    val query               = new QueryParser("title", analyzer).parse("title:lucene")
    val hitsPerPage         = 10
    val reader: IndexReader = DirectoryReader.open(index)
    val searcher            = new IndexSearcher(reader)
    val docs                = searcher.search(query, hitsPerPage)
    val hits                = docs.scoreDocs.toIndexedSeq
    println("Found " + hits.length + " hits.")
    hits.foreach { doc =>
      val document = searcher.doc(doc.doc)
      println(document)
    }
  }

  private def addDoc(w: IndexWriter, title: String, isbn: String): Unit = {
    val doc = new Document()
    doc.add(new TextField("title", title, Field.Store.YES))
    doc.add(new StringField("isbn", isbn, Field.Store.YES))
    w.addDocument(doc)
  }
}
