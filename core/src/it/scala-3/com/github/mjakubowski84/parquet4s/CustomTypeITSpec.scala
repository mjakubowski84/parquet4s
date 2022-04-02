package com.github.mjakubowski84.parquet4s

import com.github.mjakubowski84.parquet4s.TypedSchemaDef
import org.apache.parquet.schema.{LogicalTypeAnnotation, MessageType, PrimitiveType, Types}
import org.scalatest.BeforeAndAfter
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import ValueImplicits.*
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.BINARY

class CustomTypeITSpec extends AnyFlatSpec with Matchers with BeforeAndAfter with TestUtils:

  object SimpleClass:
    given RequiredValueCodec[SimpleClass] with
      override protected def decodeNonNull(value: Value, configuration: ValueCodecConfiguration): SimpleClass =
        value match {
          case BinaryValue(binary) => new SimpleClass(binary.toStringUsingUTF8)
        }
      override protected def encodeNonNull(data: SimpleClass, configuration: ValueCodecConfiguration): Value =
        BinaryValue(data.value)

    given schema: TypedSchemaDef[SimpleClass] = SchemaDef
      .primitive(
        primitiveType         = PrimitiveType.PrimitiveTypeName.BINARY,
        logicalTypeAnnotation = Option(LogicalTypeAnnotation.stringType())
      )
      .typed[SimpleClass]
  end SimpleClass

  class SimpleClass(val value: String):
    override def hashCode(): Int = value.hashCode
    override def equals(obj: Any): Boolean =
      if (!obj.isInstanceOf[SimpleClass]) false
      else this.value == obj.asInstanceOf[SimpleClass].value
    override def toString: String = value
  end SimpleClass

  object CaseClass:
    given RequiredValueCodec[CaseClass] with
      override protected def decodeNonNull(value: Value, configuration: ValueCodecConfiguration): CaseClass =
        value match {
          case BinaryValue(binary) => CaseClass(binary.toStringUsingUTF8)
        }
      override protected def encodeNonNull(data: CaseClass, configuration: ValueCodecConfiguration): Value =
        BinaryValue(data.value)

    given schema: TypedSchemaDef[CaseClass] = SchemaDef
      .primitive(
        primitiveType         = PrimitiveType.PrimitiveTypeName.BINARY,
        logicalTypeAnnotation = Option(LogicalTypeAnnotation.stringType())
      )
      .typed[CaseClass]
  end CaseClass

  case class CaseClass(value: String)

  object ComplexCaseClass:
    given RequiredValueCodec[ComplexCaseClass] with
      override protected def decodeNonNull(value: Value, configuration: ValueCodecConfiguration): ComplexCaseClass =
        value match {
          case record: RowParquetRecord =>
            (record.get[String]("i", configuration), record.get[String]("j", configuration)) match {
              case (Some(i), Some(j)) =>
                ComplexCaseClass(i.toInt, j.toInt)
              case _ =>
                throw new RuntimeException("Invalid data for ComplexCaseClass")
            }
        }
      override protected def encodeNonNull(data: ComplexCaseClass, configuration: ValueCodecConfiguration): Value =
        RowParquetRecord("i" -> data.x.toString.value, "j" -> data.y.toString.value)

    given schema: TypedSchemaDef[ComplexCaseClass] = SchemaDef
      .group(
        SchemaDef.primitive(
          primitiveType         = PrimitiveType.PrimitiveTypeName.BINARY,
          logicalTypeAnnotation = Option(LogicalTypeAnnotation.stringType())
        )("i"),
        SchemaDef.primitive(
          primitiveType         = PrimitiveType.PrimitiveTypeName.BINARY,
          logicalTypeAnnotation = Option(LogicalTypeAnnotation.stringType())
        )("j")
      )
      .typed[ComplexCaseClass]
  end ComplexCaseClass

  case class ComplexCaseClass(x: Int, y: Int)

  case class Data(a: SimpleClass, b: CaseClass, c: ComplexCaseClass)

  val typed = Set(
    Data(
      new SimpleClass("simple"),
      CaseClass("case"),
      ComplexCaseClass(1, 2)
    )
  )
  val generic = Set(
    RowParquetRecord(
      "a" -> "simple".value,
      "b" -> "case".value,
      "c" -> RowParquetRecord("i" -> "1".value, "j" -> "2".value)
    )
  )
  val schema: MessageType = Message(None, SimpleClass.schema("a"), CaseClass.schema("b"), ComplexCaseClass.schema("c"))

  before {
    clearTemp()
  }

  "Custom types" should "be properly written" in {
    // write
    ParquetWriter.of[Data].writeAndClose(tempPath.append("test.parquet"), typed)

    // read
    val readData = ParquetReader.generic.read(tempPath)
    try readData.toSet should be(generic)
    finally readData.close()
  }

  it should "be properly read" in {
    // write
    ParquetWriter.generic(schema).writeAndClose(tempPath.append("test.parquet"), generic)

    // read
    val readData = ParquetReader.as[Data].read(tempPath)
    try readData.toSet should be(typed)
    finally readData.close()
  }

end CustomTypeITSpec
