package com.github.mjakubowski84.parquet4s

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import scala.util.Using

class RecordFilterSpec extends AnyFlatSpec with Matchers {

  private case class Data(i: Int)

  private val data = (0 to 10).map(Data(_))

  "RecordFilter" should "filter data by record index" in {
    val outFile = InMemoryOutputFile(initBufferSize = 1024)
    ParquetWriter.of[Data].writeAndClose(outFile, data)
    val inFile = outFile.toInputFile
    Using.resource(ParquetReader.as[Data].filter(RecordFilter(i => i >= 1 && i < 10)).read(inFile)) { iterable =>
      val result = iterable.toVector
      result should have size 9
      result.head should be(Data(1))
      result.last should be(Data(9))
    }
  }

}
