package com.github.mjakubowski84.parquet4s

import com.github.mjakubowski84.parquet4s.ValueImplicits.*
import org.apache.parquet.schema.MessageType
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.nio.file.Files

class ProjectionItSpec extends AnyFlatSpec with Matchers {


  case class Full(a: String, b: Int, c: Double)
  case class Partial(b: Int)

  case class FullElem(x: Int, y: String)
  case class FullNested(b: List[FullElem], c: Boolean)
  case class FullComplex(a: String, nested: FullNested)

  case class PartialElem(x: Int)
  case class PartialNested(b: List[PartialElem])
  case class PartialComplex(nested: PartialNested)

  val tempPath: Path = Path(Files.createTempDirectory("example"))

  "Parquet reader with partial projection" should "read the subset of fields from written simple file" in {
    val filePath = tempPath.append("simple.parquet")
    val in = List(
      Full(a = "x", b = 1, c = 1.0),
      Full(a = "y", b = 2, c = 1.1),
      Full(a = "z", b = 3, c = 1.2)
    )
    ParquetWriter.of[Full].writeAndClose(filePath, in)

    val partialSchema = ParquetSchemaResolver.resolveSchema[Partial]

    val outRecords = ParquetReader.projectedGeneric(partialSchema).read(filePath)

    try {
      outRecords should contain theSameElementsAs List(
        RowParquetRecord("b" -> 1.value),
        RowParquetRecord("b" -> 2.value),
        RowParquetRecord("b" -> 3.value)
      )
    } finally {
      outRecords.close()
    }
  }

  it should "read the subset of fields from written complex file" in {
    val filePath = tempPath.append("complex.parquet")
    val in = List(
      FullComplex(a = "A", nested = FullNested(b = List(FullElem(1, "a"), FullElem(2, "b"), FullElem(3, "c")), c = true))
    )
    ParquetWriter.of[FullComplex].writeAndClose(filePath, in)

    val partialSchema = ParquetSchemaResolver.resolveSchema[PartialComplex]

    val outRecords = ParquetReader.projectedGeneric(partialSchema).read(filePath)

    try {
      outRecords should contain theSameElementsAs List(
        RowParquetRecord("nested" -> RowParquetRecord("b" -> ListParquetRecord(
          RowParquetRecord("x" -> 1.value),
          RowParquetRecord("x" -> 2.value),
          RowParquetRecord("x" -> 3.value)
        )))
      )
    } finally {
      outRecords.close()
    }
  }

}
