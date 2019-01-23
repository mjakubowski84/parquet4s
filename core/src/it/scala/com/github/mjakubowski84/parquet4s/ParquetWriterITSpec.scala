package com.github.mjakubowski84.parquet4s

import org.scalatest._

object ParquetWriterITSpec {

  case class BasicPrimitives(string: String, int: Int, long: Long, float: Float, double: Double, boolean: Boolean)
  case class Collections(seq: Seq[Int], list: List[Int], vector: Vector[Int], set: Set[Int], array: Array[Int])
  case class Options(nameOpt: Option[String], longOpt: Option[Long])
  case class Nested(int: Int)
  case class ContainsNested(nested: Nested)
  case class ContainsNestedOption(nestedOption: Option[Nested])
  case class ContainsNestedCollection(nestedList: List[Nested])

}

class ParquetWriterITSpec extends FlatSpec
  with Matchers
  with BeforeAndAfter
  with SparkHelper
  with Inspectors {

  import ParquetWriterITSpec._

  before {
    clearTemp()
  }

  def defaultAssertion[Data](l: Seq[Data], r: Seq[Data]): Assertion = l should be(r)
  def fixture[Data : ParquetWriter : ParquetRecordDecoder](data: Seq[Data],
                                                           test: (Seq[Data], Seq[Data]) => Assertion = defaultAssertion _
                                                          ): Assertion = {
    ParquetWriter.write(tempPathString, data)

    val dataIterable = ParquetReader.read[Data](tempPathString)
    try test(dataIterable.toSeq, data) finally {
      dataIterable.close()
    }
  }

  it should "write data with primitive values" in fixture(Seq(BasicPrimitives(
    string = "string", int = 1, long = 2l, float = 3.33f, double = 4.44d, boolean = true
  )))

  it should "write data with collections" in fixture[Collections](
    Seq(
      Collections(seq = Seq(1), list = List(2), vector = Vector(3), set = Set(4), array = Array(5)),
      Collections(seq = Seq.empty, list = List.empty, vector = Vector.empty, set = Set.empty, array = Array.empty)
    ), { case (result, expected) =>
      forAll(result.zip(expected)) { case (resultEntry, expectedEntry) =>
        resultEntry.seq should be(expectedEntry.seq)
        resultEntry.list should be(expectedEntry.list)
        resultEntry.vector should be(expectedEntry.vector)
        resultEntry.set should be(expectedEntry.set)
        resultEntry.array should contain theSameElementsInOrderAs expectedEntry.array
      }
    }
  )

  it should "write data with options" in fixture(
    Seq(Options(Some("Hello"), Some(1)), Options(None, None))
  )

  it should "write data with nested class" in fixture(
    Seq(ContainsNested(Nested(1)))
  )

  it should "write data with optional nested class" in fixture(
    Seq(ContainsNestedOption(Some(Nested(1))), ContainsNestedOption(None))
  )

  it should "write data with nested collection" in fixture(
    Seq(
      ContainsNestedCollection(List(Nested(1), Nested(2), Nested(3))),
      ContainsNestedCollection(List.empty)
    )
  )

}
