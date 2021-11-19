package com.github.mjakubowski84.parquet4s

import com.github.mjakubowski84.parquet4s.ValueImplicits.*
import org.apache.hadoop.fs.FileUtil
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.io.File
import java.nio.file.Files

class ProjectionItSpec extends AnyFlatSpec with Matchers with BeforeAndAfterAll {

  case class Full(a: String, b: Int, c: Double)
  case class Partial(b: Int)

  case class FullElem(x: Int, y: String)
  case class FullNested(b: List[FullElem], c: Boolean)
  case class FullComplex(a: String, nested: FullNested)

  case class PartialElem(x: Int)
  case class PartialNested(b: List[PartialElem])
  case class PartialComplex(nested: PartialNested)

  val tempPath: Path        = Path(Files.createTempDirectory("example"))
  val simpleFilePath: Path  = tempPath.append("simple.parquet")
  val complexFilePath: Path = tempPath.append("complex.parquet")
  val simpleData = List(
    Full(a = "x", b = 1, c = 1.0),
    Full(a = "y", b = 2, c = 1.1),
    Full(a = "z", b = 3, c = 1.2)
  )
  val complexData = List(
    FullComplex(
      a      = "A",
      nested = FullNested(b = List(FullElem(1, "a"), FullElem(2, "b"), FullElem(3, "c")), c = true)
    )
  )

  override def beforeAll(): Unit = {
    ParquetWriter.of[Full].writeAndClose(simpleFilePath, simpleData)
    ParquetWriter.of[FullComplex].writeAndClose(complexFilePath, complexData)
  }

  override def afterAll(): Unit =
    FileUtil.fullyDelete(new File(tempPath.toUri))

  "Parquet reader with partial projection" should "read the subset of fields from written simple file" in {
    val partialSchema = ParquetSchemaResolver.resolveSchema[Partial]

    val records = ParquetReader.projectedGeneric(partialSchema).read(simpleFilePath)

    try records should contain theSameElementsAs List(
      RowParquetRecord("b" -> 1.value),
      RowParquetRecord("b" -> 2.value),
      RowParquetRecord("b" -> 3.value)
    )
    finally records.close()
  }

  it should "read the subset of fields from written complex file" in {
    val partialSchema = ParquetSchemaResolver.resolveSchema[PartialComplex]

    val records = ParquetReader.projectedGeneric(partialSchema).read(complexFilePath)

    try records should contain theSameElementsAs List(
      RowParquetRecord(
        "nested" -> RowParquetRecord(
          "b" -> ListParquetRecord(
            RowParquetRecord("x" -> 1.value),
            RowParquetRecord("x" -> 2.value),
            RowParquetRecord("x" -> 3.value)
          )
        )
      )
    )
    finally records.close()
  }

  it should "read the subset of fields from written complex file when using column projection" in {
    val records =
      ParquetReader.projectedGeneric(Col("a").as[String], Col("nested").as[PartialNested]).read(complexFilePath)

    try records should contain theSameElementsAs List(
      RowParquetRecord(
        "a" -> "A".value,
        "nested" -> RowParquetRecord(
          "b" -> ListParquetRecord(
            RowParquetRecord("x" -> 1.value),
            RowParquetRecord("x" -> 2.value),
            RowParquetRecord("x" -> 3.value)
          )
        )
      )
    )
    finally records.close()
  }

  it should "apply aliases to projected field names" in {
    val records = ParquetReader.projectedGeneric(Col("a").as[String].alias("alias")).read(simpleFilePath)
    try records should contain theSameElementsAs List(
      RowParquetRecord("alias" -> "x".value),
      RowParquetRecord("alias" -> "y".value),
      RowParquetRecord("alias" -> "z".value)
    )
    finally records.close()
  }

  it should "be able to project to nested field" in {
    val records = ParquetReader.projectedGeneric(Col("nested.c").as[Boolean]).read(complexFilePath)
    try records should contain theSameElementsAs List(
      RowParquetRecord("c" -> true.value)
    )
    finally records.close()
  }

  it should "be able to project to nested field and give a new name to it" in {
    val records = ParquetReader.projectedGeneric(Col("nested.c").as[Boolean].alias("x")).read(complexFilePath)
    try records should contain theSameElementsAs List(
      RowParquetRecord("x" -> true.value)
    )
    finally records.close()
  }

  it should "allow to project the same primitive field more than once" in {
    val records = ParquetReader
      .projectedGeneric(
        Col("a").as[String],
        Col("a").as[String],
        Col("a").as[Long].alias("x") // even though Long is used here - it is discarded and first `as` is used
      )
      .read(complexFilePath)
    try records should contain theSameElementsAs List(
      RowParquetRecord("a" -> "A".value, "a" -> "A".value, "x" -> "A".value)
    )
    finally records.close()
  }

  it should "allow to project the same complex field more than once" in {
    val records = ParquetReader
      .projectedGeneric(
        Col("nested").as[PartialNested], // the first `as` is selected for a schema of repeated column
        Col("nested").as[FullNested],
        Col("nested").as[FullNested].alias("renamed")
      )
      .read(complexFilePath)

    val expectedNestedRecord = RowParquetRecord(
      "b" -> ListParquetRecord(
        RowParquetRecord("x" -> 1.value),
        RowParquetRecord("x" -> 2.value),
        RowParquetRecord("x" -> 3.value)
      )
    )

    try records.toList should be(
      List(
        RowParquetRecord(
          "nested" -> expectedNestedRecord,
          "nested" -> expectedNestedRecord,
          "renamed" -> expectedNestedRecord
        )
      )
    )
    finally records.close()
  }

}
