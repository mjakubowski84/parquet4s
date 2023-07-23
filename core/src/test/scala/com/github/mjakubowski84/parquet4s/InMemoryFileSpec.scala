package com.github.mjakubowski84.parquet4s

import org.apache.commons.lang3.RandomStringUtils
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.nio.file.Files

class InMemoryFileSpec extends AnyFlatSpec with Matchers {
  it should "write to in-memory output file" in {
    case class Data(id: Int, text: String)

    val count = 100
    val data  = (1 to count).map(i => Data(id = i, text = RandomStringUtils.randomPrint(4)))
    val file  = InMemoryOutputFile("test")

    // write
    ParquetWriter.of[Data].writeAndClose(file, data)

    val inputFile = Files.createTempFile("in-memory-output-file-test", ".parquet")
    Files.write(inputFile, file.toByteArray)

    // read
    val readData = ParquetReader.as[Data].read(Path(inputFile))
    try readData.toSeq shouldBe data
    finally readData.close()
  }

  it should "read from in-memory input file" in {
    case class Data(id: Int, text: String)

    val count      = 100
    val data       = (1 to count).map(i => Data(id = i, text = RandomStringUtils.randomPrint(4)))
    val outputFile = InMemoryOutputFile("test")

    // write
    ParquetWriter.of[Data].writeAndClose(outputFile, data)

    val inputFile = InMemoryInputFile.fromBytesUnsafe(outputFile.toByteArray)

    // read
    val readData = ParquetReader.as[Data].read(inputFile)
    try readData.toSeq shouldBe data
    finally readData.close()
  }
}
