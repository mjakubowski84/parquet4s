package com.github.mjakubowski84.parquet4s

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.apache.parquet.schema.PrimitiveType
import org.apache.parquet.schema.LogicalTypeAnnotation

import scala.util.Using

class DecimalValueSpec extends AnyFlatSpec with Matchers {

  case class Data(decimal: BigDecimal)
  val data = Seq(Data(BigDecimal.decimal(1.2f)))

  "Decimal value" should "read from binary" in {

    val outputFile = InMemoryOutputFile(initBufferSize = 1024)

    // write
    ParquetWriter
      .generic(Message(Some("binary-dec"), TypedSchemaDef.decimalSchema("decimal")))
      .writeAndClose(
        outputFile,
        Seq(RowParquetRecord("decimal" -> BinaryValue(Decimals.binaryFromDecimal(BigDecimal.decimal(1.2f)))))
      )

    val inputFile = outputFile.toInputFile

    // read
    Using.resource(ParquetReader.as[Data].read(inputFile)) { readData =>
      readData.toSeq shouldBe data
    }
  }

  it should "read from long" in {

    val outputFile = InMemoryOutputFile(initBufferSize = 1024)

    // write
    ParquetWriter
      .generic(
        Message(
          Some("long-dec"),
          SchemaDef.primitive(
            primitiveType         = PrimitiveType.PrimitiveTypeName.INT64,
            logicalTypeAnnotation = Some(LogicalTypeAnnotation.decimalType(4, 8))
          )("decimal")
        )
      )
      .writeAndClose(outputFile, Seq(RowParquetRecord("decimal" -> LongValue(12000L))))

    val inputFile = outputFile.toInputFile

    // read
    Using.resource(ParquetReader.as[Data].read(inputFile)) { readData =>
      readData.toSeq shouldBe data
    }
  }

}
