package com.github.mjakubowski84.parquet4s

import org.apache.parquet.schema.{OriginalType, PrimitiveType}
import org.scalatest.{FlatSpec, Matchers}
import TestCases._
import ParquetSchemaResolver.resolveSchema


class ParquetSchemaResolverSpec extends FlatSpec with Matchers {

  "ParquetSchemaResolver" should "resolve schema for type with no fields" in {
    resolveSchema[Empty] should be(Message())
  }

  it should "resolve schema for type with primitives" in {
    resolveSchema[Primitives] should be(Message(
      PrimitiveSchemaDef(PrimitiveType.PrimitiveTypeName.BOOLEAN)("boolean"),
      PrimitiveSchemaDef(PrimitiveType.PrimitiveTypeName.INT32, originalType = Some(OriginalType.INT_32))("int"),
      PrimitiveSchemaDef(PrimitiveType.PrimitiveTypeName.INT64, originalType = Some(OriginalType.INT_64))("long"),
      PrimitiveSchemaDef(PrimitiveType.PrimitiveTypeName.FLOAT)("float"),
      PrimitiveSchemaDef(PrimitiveType.PrimitiveTypeName.DOUBLE)("double"),
      PrimitiveSchemaDef(PrimitiveType.PrimitiveTypeName.BINARY, required = false, originalType = Some(OriginalType.UTF8))("string"),
      PrimitiveSchemaDef(PrimitiveType.PrimitiveTypeName.INT32, originalType = Some(OriginalType.INT_16))("short"),
      PrimitiveSchemaDef(PrimitiveType.PrimitiveTypeName.INT32, originalType = Some(OriginalType.INT_8))("byte")
    ))
  }

  it should "resolve schema for type containing optional types" in {
    resolveSchema[ContainsOption] should be(Message(
      PrimitiveSchemaDef(PrimitiveType.PrimitiveTypeName.INT32, required = false, Some(OriginalType.INT_32))("optional")
    ))
  }

  it should "resolve schema for type containing collections of primitives" in {
    val intSchemaDef = PrimitiveSchemaDef(PrimitiveType.PrimitiveTypeName.INT32, originalType = Some(OriginalType.INT_32))
    resolveSchema[Collections] should be(Message(
      ListGroupSchemaDef.optional(intSchemaDef)("list"),
      ListGroupSchemaDef.optional(intSchemaDef)("seq"),
      ListGroupSchemaDef.optional(intSchemaDef)("vector"),
      ListGroupSchemaDef.optional(intSchemaDef)("set"),
      ListGroupSchemaDef.optional(intSchemaDef)("array")
    ))
  }

  it should "resolve schema for type containing collection of optional primitives" in {
    resolveSchema[ContainsCollectionOfOptionalPrimitives] should be(Message(
      ListGroupSchemaDef.optional(
        PrimitiveSchemaDef(PrimitiveType.PrimitiveTypeName.INT32, required = false, originalType = Some(OriginalType.INT_32))
      )("list")
    ))
  }

  it should "resolve schema for type containing collection of collections of primitives" in {
    resolveSchema[ContainsCollectionOfCollections] should be(Message(
      ListGroupSchemaDef.optional(
        ListGroupSchemaDef.optional(
          PrimitiveSchemaDef(PrimitiveType.PrimitiveTypeName.INT32, originalType = Some(OriginalType.INT_32))
        )
      )("listOfSets")
    ))
  }

  it should "resolve schema for type containing map of primitives" in {
    resolveSchema[ContainsMapOfPrimitives] should be(Message(
      MapSchemaDef.optional(
        PrimitiveSchemaDef(PrimitiveType.PrimitiveTypeName.BINARY, originalType = Some(OriginalType.UTF8)),
        PrimitiveSchemaDef(PrimitiveType.PrimitiveTypeName.INT32, originalType = Some(OriginalType.INT_32))
      )("map")
    ))
  }

  it should "resolve schema for type containing map of optional primitives" in {
    resolveSchema[ContainsMapOfOptionalPrimitives] should be(Message(
      MapSchemaDef.optional(
        PrimitiveSchemaDef(PrimitiveType.PrimitiveTypeName.BINARY, originalType = Some(OriginalType.UTF8)),
        PrimitiveSchemaDef(PrimitiveType.PrimitiveTypeName.INT32, required = false, originalType = Some(OriginalType.INT_32))
      )("map")
    ))
  }

  it should "resolve schema for type containing map of collections of primitives" in {
    resolveSchema[ContainsMapOfCollectionsOfPrimitives] should be(Message(
      MapSchemaDef.optional(
        PrimitiveSchemaDef(PrimitiveType.PrimitiveTypeName.BINARY, originalType = Some(OriginalType.UTF8)),
        ListGroupSchemaDef.optional(
          PrimitiveSchemaDef(PrimitiveType.PrimitiveTypeName.INT32, originalType = Some(OriginalType.INT_32))
        )
      )("map")
    ))
  }

  it should "resolve schema for type containing nested class" in {
    resolveSchema[ContainsNestedClass] should be(Message(
      GroupSchemaDef.optional(
        PrimitiveSchemaDef(PrimitiveType.PrimitiveTypeName.INT32, originalType = Some(OriginalType.INT_32))("int")
      )("nested")
    ))
  }

  it should "resolve schema for type containing optional nested class" in {
    resolveSchema[ContainsOptionalNestedClass] should be(Message(
      GroupSchemaDef.optional(
        PrimitiveSchemaDef(PrimitiveType.PrimitiveTypeName.INT32, originalType = Some(OriginalType.INT_32))("int")
      )("nestedOptional")
    ))
  }

  it should "resolve schema for type containing collection of nested class" in {
    val nestedClassSchemaDef = GroupSchemaDef.optional(
      PrimitiveSchemaDef(PrimitiveType.PrimitiveTypeName.INT32, originalType = Some(OriginalType.INT_32))("int")
    )
    resolveSchema[CollectionsOfNestedClass] should be(Message(
      ListGroupSchemaDef.optional(nestedClassSchemaDef)("list"),
      ListGroupSchemaDef.optional(nestedClassSchemaDef)("seq"),
      ListGroupSchemaDef.optional(nestedClassSchemaDef)("vector"),
      ListGroupSchemaDef.optional(nestedClassSchemaDef)("set"),
      ListGroupSchemaDef.optional(nestedClassSchemaDef)("array")
    ))
  }

  it should "resolve schema for type containing map of nested classes" in {
    resolveSchema[ContainsMapOfNestedClassAsValue] should be(Message(
      MapSchemaDef.optional(
        PrimitiveSchemaDef(PrimitiveType.PrimitiveTypeName.BINARY, originalType = Some(OriginalType.UTF8)),
        GroupSchemaDef.optional(
          PrimitiveSchemaDef(PrimitiveType.PrimitiveTypeName.INT32, originalType = Some(OriginalType.INT_32))("int")
        )
      )("nested")
    ))
  }

  it should "resolve schema for type containing map of optional nested classes" in {
    resolveSchema[ContainsMapOfOptionalNestedClassAsValue] should be(Message(
      MapSchemaDef.optional(
        PrimitiveSchemaDef(PrimitiveType.PrimitiveTypeName.BINARY, originalType = Some(OriginalType.UTF8)),
        GroupSchemaDef.optional(
          PrimitiveSchemaDef(PrimitiveType.PrimitiveTypeName.INT32, originalType = Some(OriginalType.INT_32))("int")
        )
      )("nested")
    ))
  }

  it should "resolve schema for type containing map of collections of nested classes" in {
    resolveSchema[ContainsMapOfCollectionsOfNestedClassAsValue] should be(Message(
      MapSchemaDef.optional(
        PrimitiveSchemaDef(PrimitiveType.PrimitiveTypeName.BINARY, originalType = Some(OriginalType.UTF8)),
        ListGroupSchemaDef.optional(
          GroupSchemaDef.optional(
            PrimitiveSchemaDef(PrimitiveType.PrimitiveTypeName.INT32, originalType = Some(OriginalType.INT_32))("int")
          )
        )
      )("nested")
    ))
  }

}
