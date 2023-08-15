package com.github.mjakubowski84.parquet4s

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import com.github.mjakubowski84.parquet4s.ScalaPBImplicits.*

class Parquet4sScalaPBSpec extends AnyFlatSpec with Matchers {

  def testWithData(newData: Int => Data): Unit = {
    val data = (1 to 100).map(newData)

    val outFile = InMemoryOutputFile(initBufferSize = 4800)
    ParquetWriter.of[Data].writeAndClose(outFile, data)

    val inFile = InMemoryInputFile.fromBytes(outFile.take())
    ParquetReader.as[Data].read(inFile).toSeq shouldBe data
  }

  "parquet4s-scalapb" should "work with primitive types" in {
    testWithData(i => Data(bool = i % 2 == 0))
    testWithData(i => Data(int = i))
    testWithData(i => Data(long = i.toLong))
    testWithData(i => Data(float = i.toFloat))
    testWithData(i => Data(double = i.toDouble))
    testWithData(i => Data(text = i.toString))
    testWithData(i => Data(abc = Data.ABC.fromValue(i % 3)))
  }

  it should "work with message types" in {
    testWithData(i => Data(inner = Some(Data.Inner(i.toString))))
  }

  it should "work with unrecognized enum values" in {
    testWithData(i => Data(abc = Data.ABC.fromValue(i % 5)))
  }

  it should "work with map types" in {
    testWithData(i => Data(map = Map("original" -> i, "doubled" -> 2 * i)))
    testWithData(i => Data(enumMap = Map(i -> Data.ABC.fromValue(i % 5))))
    testWithData(i => Data(msgMap = Map(i.toLong -> Data.Inner(text = "level1"))))
  }

  it should "work with list types" in {
    testWithData(i => Data(boolList = (i to i + 100).map(_ % 2 == 0)))
    testWithData(i => Data(intList = i to i + 100))
    testWithData(i => Data(longList = (i to i + 100).map(_.toLong)))
    testWithData(i => Data(floatList = (i to i + 100).map(_.toFloat)))
    testWithData(i => Data(doubleList = (i to i + 100).map(_.toDouble)))
    testWithData(i => Data(textList = (i to i + 100).map(_.toString)))
    testWithData(i => Data(enumList = (i to i + 100).map(Data.ABC.fromValue)))
    testWithData(i => Data(msgList = (i to i + 100).map(i => Data.Inner(i.toString))))
  }

}
