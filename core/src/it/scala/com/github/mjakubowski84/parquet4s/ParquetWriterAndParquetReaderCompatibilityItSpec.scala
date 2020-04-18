package com.github.mjakubowski84.parquet4s

import com.github.mjakubowski84.parquet4s.Case.CaseDef
import com.github.mjakubowski84.parquet4s.CompatibilityParty._
import org.scalatest.matchers.should.Matchers
import org.scalatest.BeforeAndAfter
import org.scalatest.freespec.AnyFreeSpec

class ParquetWriterAndParquetReaderCompatibilityItSpec extends
  AnyFreeSpec
    with Matchers
    with BeforeAndAfter
    with TestUtils {

  before {
    clearTemp()
  }

  private def runTestCase(testCase: CaseDef): Unit = {
    testCase.description in {
      ParquetWriter.writeAndClose(tempPathString, testCase.data)(testCase.writerFactory)
      val parquetIterable = ParquetReader.read(tempPathString)(testCase.reader)
      try {
        parquetIterable should contain theSameElementsAs testCase.data
      } finally {
        parquetIterable.close()
      }
    }
  }

  "Spark should be able to read file saved by ParquetWriter if the file contains" - {
    CompatibilityTestCases.cases(Writer, Reader).foreach(runTestCase)
  }

}
