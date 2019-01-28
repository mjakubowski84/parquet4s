package com.github.mjakubowski84.parquet4s

import com.github.mjakubowski84.parquet4s.CompatibilityParty._
import org.scalatest.{BeforeAndAfter, FreeSpec, Matchers}

class ParquetWriterAndSparkCompatibilityItSpec extends
  FreeSpec
    with Matchers
    with BeforeAndAfter
    with SparkHelper {

  before {
    clearTemp()
  }

  "Spark should be able to read file saved by ParquetWriter if the file contains" - {
    CompatibilityTestCases.cases(Writer, Spark).foreach { testCase =>
      testCase.description in {
        ParquetWriter.write(tempPathString, testCase.data)(testCase.writer)
        readFromTemp(testCase.typeTag) should contain theSameElementsAs testCase.data
      }
    }
  }

}
