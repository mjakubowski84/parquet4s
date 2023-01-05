package com.github.mjakubowski84.parquet4s.lucene

import com.github.mjakubowski84.parquet4s.LogicalTypes.StringType
import com.github.mjakubowski84.parquet4s.lucene.LuceneITSpec.{data, schema}
import com.github.mjakubowski84.parquet4s.{
  BinaryValue,
  Col,
  Message,
  ParquetWriter,
  Path,
  RowParquetRecord,
  ValueCodecConfiguration
}
import org.apache.hadoop.fs.FileUtil
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.BINARY
import org.apache.parquet.schema.Type.Repetition.OPTIONAL
import org.apache.parquet.schema.{MessageType, Types}
import org.scalatest.{BeforeAndAfter, Inspectors}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.io.File
import java.nio.file.Files
import scala.util.Random

object LuceneITSpec {

  private val dictionary: Seq[String] = Seq(
    "parquet",
    "hadoop",
    "akka",
    "fs2",
    "lucene",
    "storage",
    "read",
    "write",
    "search",
    "query"
  )
  private val dictSize = dictionary.length
  val data: Seq[RowParquetRecord] = Seq.iterate(RowParquetRecord.emptyWithSchema("text"), 1024) { record =>
    val text = LazyList.continually(dictionary(Random.nextInt(dictSize))).take(4).mkString(" ")
    record.updated("text", BinaryValue(text))
  }
  val schema: MessageType = Message.apply(None, Types.primitive(BINARY, OPTIONAL).as(StringType).named("text"))
}

class LuceneITSpec extends AnyFlatSpec with Matchers with BeforeAndAfter with Inspectors {

  private val tempPath: Path = Path(Files.createTempDirectory("testdir")).append("luceneitspec")
  private val filePath       = tempPath.append("file.parquet")
  private val vcc            = ValueCodecConfiguration.Default

  before {
    FileUtil.fullyDelete(new File(tempPath.toUri))
  }

  "Test" should "work" in {

    ParquetWriter.generic(schema).writeAndClose(filePath, data)

    Indexer().index(filePath, Col("text").as[String])

    val results = LuceneRecordFilter(
      queryString = "text:parquet",
      path        = tempPath
    ).flatMap(_.get[String]("text", vcc))

    forAll(results)(_ should include("parquet"))

    val expectedCount = data
      .flatMap(_.get[String]("text", vcc))
      .filterNot(_ == null)
      .count(_.contains("parquet"))

    results should have size expectedCount
  }

}
