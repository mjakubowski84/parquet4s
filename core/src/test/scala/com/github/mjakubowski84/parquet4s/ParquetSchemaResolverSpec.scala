package com.github.mjakubowski84.parquet4s

import org.apache.parquet.schema.{OriginalType, PrimitiveType}
import org.scalatest.{FlatSpec, Matchers}

class ParquetSchemaResolverSpec extends FlatSpec with Matchers {

  "ParquetSchemaResolver" should "resolve schema for type with no fields" in {
    case class Empty()

    ParquetSchemaResolver.resolveSchema[Empty] should be(Message())
  }

  it should "resolve schema for type with primitives" in {
    case class Primitives(int: Int, string: String)

    ParquetSchemaResolver.resolveSchema[Primitives] should be(Message(
      PrimitiveSchemaDef(PrimitiveType.PrimitiveTypeName.INT32, required = true, Some(OriginalType.INT_32))("int"),
      PrimitiveSchemaDef(PrimitiveType.PrimitiveTypeName.BINARY, required = false, Some(OriginalType.UTF8))("string")
    ))
  }

  it should "resolve schema for type containing optional types" in {
    case class Row(optionalField: Option[Int])

    ParquetSchemaResolver.resolveSchema[Row] should be(Message(
      PrimitiveSchemaDef(PrimitiveType.PrimitiveTypeName.INT32, required = false, Some(OriginalType.INT_32))("optionalField")
    ))
  }

  it should "resolve schema for type containing collection of primitives" in {
    case class Row(list: List[Int])

    ParquetSchemaResolver.resolveSchema[Row] should be(Message(
      ListGroupSchemaDef(
        PrimitiveSchemaDef(PrimitiveType.PrimitiveTypeName.INT32, required = true, Some(OriginalType.INT_32))("element")
      )("list")
    ))
  }

  it should "resolve schema for type containing collection of optional primitives" in {
    case class Row(list: List[Option[Int]])

    ParquetSchemaResolver.resolveSchema[Row] should be(Message(
      ListGroupSchemaDef(
        PrimitiveSchemaDef(PrimitiveType.PrimitiveTypeName.INT32, required = false, Some(OriginalType.INT_32))("element")
      )("list")
    ))
  }

}
