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
        Seq(
          RowParquetRecord("decimal" -> BinaryValue(DecimalFormat.Default.binaryFromDecimal(BigDecimal.decimal(1.2f))))
        )
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

  it should "read from int" in {

    val outputFile = InMemoryOutputFile(initBufferSize = 1024)

    // write
    ParquetWriter
      .generic(
        Message(
          Some("long-dec"),
          SchemaDef.primitive(
            primitiveType         = PrimitiveType.PrimitiveTypeName.INT32,
            logicalTypeAnnotation = Some(LogicalTypeAnnotation.decimalType(4, 8))
          )("decimal")
        )
      )
      .writeAndClose(outputFile, Seq(RowParquetRecord("decimal" -> IntValue(12000))))

    val inputFile = outputFile.toInputFile

    // read
    Using.resource(ParquetReader.as[Data].read(inputFile)) { readData =>
      readData.toSeq shouldBe data
    }
  }

  it should "read rescale on read if requested" in {

    val format = DecimalFormat.intFormat(scale = 6, precision = 12, rescaleOnRead = true)

    val outputFile = InMemoryOutputFile(initBufferSize = 1024)

    // write
    ParquetWriter
      .generic(
        Message(
          Some("long-dec"),
          SchemaDef.primitive(
            primitiveType         = PrimitiveType.PrimitiveTypeName.INT32,
            logicalTypeAnnotation = Some(LogicalTypeAnnotation.decimalType(4, 8))
          )("decimal")
        )
      )
      .writeAndClose(outputFile, Seq(RowParquetRecord("decimal" -> IntValue(12000))))

    val inputFile = outputFile.toInputFile

    // read
    import format.Implicits.*
    Using.resource(ParquetReader.as[Data].read(inputFile)) { readData =>
      readData.head.decimal.toString shouldBe "1.200000"
    }
  }

  it should "read not rescale on read if requested" in {

    val format = DecimalFormat.intFormat(scale = 6, precision = 12, rescaleOnRead = false)

    val outputFile = InMemoryOutputFile(initBufferSize = 1024)

    // write
    ParquetWriter
      .generic(
        Message(
          Some("long-dec"),
          SchemaDef.primitive(
            primitiveType         = PrimitiveType.PrimitiveTypeName.INT32,
            logicalTypeAnnotation = Some(LogicalTypeAnnotation.decimalType(4, 8))
          )("decimal")
        )
      )
      .writeAndClose(outputFile, Seq(RowParquetRecord("decimal" -> IntValue(12000))))

    val inputFile = outputFile.toInputFile

    // read
    import format.Implicits.*
    Using.resource(ParquetReader.as[Data].read(inputFile)) { readData =>
      readData.head.decimal.toString shouldBe "1.2000"
    }
  }

}
