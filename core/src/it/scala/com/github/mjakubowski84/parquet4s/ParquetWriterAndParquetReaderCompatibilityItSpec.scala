package com.github.mjakubowski84.parquet4s

import com.github.mjakubowski84.parquet4s.Case.CaseDef
import com.github.mjakubowski84.parquet4s.CompatibilityParty.*
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
      ParquetWriter
        .of(testCase.encoder, testCase.resolver)
        .writeAndClose(tempPath, testCase.data)
      val parquetIterable = ParquetReader.as[testCase.DataType].read(tempPath)(testCase.decoder)
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
