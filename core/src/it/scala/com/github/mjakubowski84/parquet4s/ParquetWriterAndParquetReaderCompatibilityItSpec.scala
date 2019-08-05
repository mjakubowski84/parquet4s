package com.github.mjakubowski84.parquet4s

import com.github.mjakubowski84.parquet4s.Case.CaseDef
import com.github.mjakubowski84.parquet4s.CompatibilityParty._
import org.scalatest.{BeforeAndAfter, FreeSpec, Matchers}

class ParquetWriterAndParquetReaderCompatibilityItSpec extends
  FreeSpec
    with Matchers
    with BeforeAndAfter
    with SparkHelper {

  before {
    clearTemp()
  }

  private def runTestCase(testCase: CaseDef, incremental: Boolean = false): Unit = {
    testCase.description in {
      if (incremental) {
        val w: IncrementalParquetWriter[testCase.DataType] = ParquetWriter.incremental(tempPathString)(testCase.writer)
        val q = testCase.data.map(Iterable(_))
        try testCase.data.map(Iterable(_)).foreach(w.write)
        finally w.close()
      } else ParquetWriter.write(tempPathString, testCase.data)(testCase.writer)
      val parquetIterable = ParquetReader.read(tempPathString)(testCase.reader)
      try {
        parquetIterable should contain theSameElementsAs testCase.data
      } finally {
        parquetIterable.close()
      }
    }
  }

  "Spark should be able to read file saved by ParquetWriter if the file contains" - {
    CompatibilityTestCases.cases(Writer, Reader).foreach(runTestCase(_))
  }

  "Behaves the same for incremental writes" - {
    CompatibilityTestCases.cases(Writer, Reader).foreach(tc => runTestCase(tc, incremental = true))
  }

}
