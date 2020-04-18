package com.github.mjakubowski84.parquet4s

import com.github.mjakubowski84.parquet4s.Case.CaseDef
import com.github.mjakubowski84.parquet4s.CompatibilityParty._
import org.scalatest.BeforeAndAfter
import org.scalatest.freespec.AnyFreeSpec
import org.scalatest.matchers.should.Matchers

class ParquetWriterAndSparkCompatibilityItSpec extends
  AnyFreeSpec
    with Matchers
    with BeforeAndAfter
    with SparkHelper {

  before {
    clearTemp()
  }

  private def runTestCase(testCase: CaseDef): Unit =
    testCase.description in {
      ParquetWriter.writeAndClose(tempPathString, testCase.data)(testCase.writerFactory)
      readFromTemp(testCase.typeTag) should contain theSameElementsAs testCase.data
    }

  "Spark should be able to read file saved by ParquetWriter if the file contains" - {
    CompatibilityTestCases.cases(Writer, Spark).foreach(runTestCase)
  }

}
