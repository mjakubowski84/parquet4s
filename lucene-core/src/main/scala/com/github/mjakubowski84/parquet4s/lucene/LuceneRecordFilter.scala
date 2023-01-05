package com.github.mjakubowski84.parquet4s.lucene

import com.github.mjakubowski84.parquet4s.{ParquetReader, Path, RecordFilter, RowParquetRecord}
import com.google.common.collect.Sets
import org.apache.lucene.analysis.standard.StandardAnalyzer
import org.apache.lucene.index.DirectoryReader
import org.apache.lucene.queryparser.classic.QueryParser
import org.apache.lucene.search.IndexSearcher
import org.apache.lucene.store.Directory
import org.apache.solr.hdfs.store.HdfsDirectory

object LuceneRecordFilter {

  val analyzer = new StandardAnalyzer()

  def apply(
      queryString: String,
      path: Path,
      readOptions: ParquetReader.Options = ParquetReader.Options()
  ): Iterable[RowParquetRecord] = {
    val query = new QueryParser("title", analyzer).parse(queryString)

    val configuration = readOptions.hadoopConf
    // TODO path
    val indexPath = path.append("lucene_index")

    val index: Directory = new HdfsDirectory(indexPath.hadoopPath, configuration)

    val hitsPerPage = 1024
    val indexReader = DirectoryReader.open(index)
    val searcher    = new IndexSearcher(indexReader)

    val docs = searcher.search(query, hitsPerPage) // TODO pagination needed???

    val hits = docs.scoreDocs.toIndexedSeq
    val results = hits
      .map { scoreDoc =>
        val document =
          indexReader.document(scoreDoc.doc, Sets.newHashSet("parquet4s_file_name", "parquet4s_record_idx"))
        val fileName =
          Option(document.get("parquet4s_file_name")).getOrElse(throw new IllegalStateException("invalid index"))
        val recordIndex =
          Option(document.getField("parquet4s_record_idx"))
            .getOrElse(throw new IllegalStateException("invalid index"))
            .numericValue()
            .longValue()
        path.append(fileName) -> recordIndex
      }
      .groupMap(_._1)(_._2)
    results.foldLeft(Iterable.empty[RowParquetRecord]) { case (results, (path, indices)) =>
      results ++ ParquetReader.generic
        .options(readOptions)
        .filter(RecordFilter(indices.toSet))
        .read(path)
    }
  }

}
