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
      implicit val resolver: ParquetSchemaResolver[testCase.DataType] = testCase.resolver
      implicit val encoder: ParquetRecordEncoder[testCase.DataType] = testCase.encoder
      if (incremental) {
        val w = ParquetWriter[testCase.DataType](tempPathString)
        try testCase.data.foreach(d => w.write(Iterable(d))) finally w.close()
      } else ParquetWriter.write(tempPathString, testCase.data)
      ParquetWriter.write(tempPathString, testCase.data)(testCase.resolver, testCase.encoder)
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

  "Incremental writes work equivalently to passing the whole Iterable" - {
    CompatibilityTestCases.cases(Writer, Reader).foreach(runTestCase(_, incremental = true))
  }

}
