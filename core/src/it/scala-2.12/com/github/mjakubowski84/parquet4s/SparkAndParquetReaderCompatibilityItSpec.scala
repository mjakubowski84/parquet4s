package com.github.mjakubowski84.parquet4s

import com.github.mjakubowski84.parquet4s.CompatibilityParty.*
import SparkAndParquetReaderCompatibilityItSpec.Partitioned
import org.scalatest.BeforeAndAfter
import org.scalatest.freespec.AnyFreeSpec
import org.scalatest.matchers.should.Matchers
import scala.annotation.nowarn
import scala.util.Using

object SparkAndParquetReaderCompatibilityItSpec {
  case class Partitioned(partition: String, s: String)
}

@nowarn
class SparkAndParquetReaderCompatibilityItSpec extends AnyFreeSpec with Matchers with BeforeAndAfter with SparkHelper {

  before {
    clearTemp()
  }

  private def runTestCase(testCase: Case.CaseDef): Unit =
    testCase.description in {
      writeToTemp(testCase.data)(testCase.typeTag)
      Using.resource(ParquetReader.as[testCase.DataType].read(tempPath)(testCase.decoder)) { parquetIterable =>
        parquetIterable should contain theSameElementsAs testCase.data
      }
    }

  "ParquetReader should be able to read file saved by Spark if the file contains" - {
    CompatibilityTestCases.cases(Spark, Reader).foreach(runTestCase)
  }

  "ParquetReader should read data partitioned by Spark" in {
    import sparkSession.implicits.*
    val data = Seq(
      Partitioned(partition = "a", s   = "a"),
      Partitioned(partition = "a=1", s = "a")
    )
    data.toDS().write.partitionBy("partition").parquet(tempPath.toString)
    Using.resource(ParquetReader.as[Partitioned].read(tempPath)) { parquetIterable =>
      parquetIterable should contain theSameElementsAs data
    }
  }

}
