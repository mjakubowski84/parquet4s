package com.github.mjakubowski84.parquet4s

import java.nio.file.Paths

import com.google.common.io.Files
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Inspectors, Matchers}

import scala.util.Random

class FilteringSpec extends FlatSpec with Matchers with BeforeAndAfterAll with Inspectors {

  case class Data(idx: Int, enum: String, flag: Boolean)

  val enum = List("a", "b", "c", "d")
  val dataSize = 1024
  val filePath: String = Paths.get(Files.createTempDir().getAbsolutePath, "file.parquet").toString

  def data: Stream[Data] =
    Stream.range(0, dataSize).map { i =>
      Data(idx = i, enum = enum(Random.nextInt(enum.size - 1)), flag = Random.nextBoolean())
    }

  override def beforeAll(): Unit = {
    super.beforeAll()
    ParquetWriter.write(filePath, data, ParquetWriter.Options(rowGroupSize = dataSize, pageSize = dataSize))
  }

  "Filtering" should "filter data by int" in {
    val boundaryValue = 512

    forAll(ParquetReader.read[Data](filePath, filterOpt = Some(Col("idx") < boundaryValue))) { dataRecord =>
      dataRecord.idx should be < boundaryValue
    }

    forAll(ParquetReader.read[Data](filePath, filterOpt = Some(Col("idx") <= boundaryValue))) { dataRecord =>
      dataRecord.idx should be <= boundaryValue
    }

    ParquetReader.read[Data](filePath, filterOpt = Some(Col("idx") === boundaryValue)).head.idx should be(boundaryValue)

    forAll(ParquetReader.read[Data](filePath, filterOpt = Some(Col("idx") >= boundaryValue))) { dataRecord =>
      dataRecord.idx should be >= boundaryValue
    }

    forAll(ParquetReader.read[Data](filePath, filterOpt = Some(Col("idx") > boundaryValue))) { dataRecord =>
      dataRecord.idx should be > boundaryValue
    }
  }

  it should "filter data by text" in {
    val boundaryValue = "c"

    forAll(ParquetReader.read[Data](filePath, filterOpt = Some(Col("enum") < boundaryValue))) { dataRecord =>
      dataRecord.enum should be < boundaryValue
    }

    forAll(ParquetReader.read[Data](filePath, filterOpt = Some(Col("enum") <= boundaryValue))) { dataRecord =>
      dataRecord.enum should be <= boundaryValue
    }

    ParquetReader.read[Data](filePath, filterOpt = Some(Col("enum") === boundaryValue)).head.enum should be(boundaryValue)

    forAll(ParquetReader.read[Data](filePath, filterOpt = Some(Col("enum") >= boundaryValue))) { dataRecord =>
      dataRecord.enum should be >= boundaryValue
    }

    forAll(ParquetReader.read[Data](filePath, filterOpt = Some(Col("enum") > boundaryValue))) { dataRecord =>
      dataRecord.enum should be > boundaryValue
    }
  }

  it should "filter data by boolean" in {
    forAll(ParquetReader.read[Data](filePath, filterOpt = Some(Col("flag") === true))) { dataRecord =>
      dataRecord.flag should be(true)
    }

    forAll(ParquetReader.read[Data](filePath, filterOpt = Some(Col("flag") === false))) { dataRecord =>
      dataRecord.flag should be(false)
    }
  }

}
