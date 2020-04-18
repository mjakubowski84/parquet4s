package com.github.mjakubowski84.parquet4s

import com.github.mjakubowski84.parquet4s.CompatibilityParty._
import org.scalatest.BeforeAndAfter
import org.scalatest.freespec.AnyFreeSpec
import org.scalatest.matchers.should.Matchers

class SparkAndParquetReaderCompatibilityItSpec extends
  AnyFreeSpec
    with Matchers
    with BeforeAndAfter
    with SparkHelper {

  before {
    clearTemp()
  }

  private def runTestCase(testCase: Case.CaseDef): Unit =
    testCase.description in {
      writeToTemp(testCase.data)(testCase.typeTag)
      val parquetIterable = ParquetReader.read(tempPathString)(testCase.reader)
      try {
        parquetIterable should contain theSameElementsAs testCase.data
      } finally {
        parquetIterable.close()
      }
    }

  "ParquetReader should be able to read file saved by Spark if the file contains" - {
    CompatibilityTestCases.cases(Spark, Reader).foreach(runTestCase)
  }

}
