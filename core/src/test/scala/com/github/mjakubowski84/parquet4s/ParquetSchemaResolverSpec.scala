package com.github.mjakubowski84.parquet4s

import org.apache.parquet.schema.{DecimalMetadata, OriginalType, PrimitiveType, Types}
import PrimitiveType.PrimitiveTypeName._
import org.apache.parquet.schema.Type.Repetition._
import org.scalatest.{FlatSpec, Matchers}
import TestCases._
import ParquetSchemaResolver.resolveSchema


class ParquetSchemaResolverSpec extends FlatSpec with Matchers {

  "ParquetSchemaResolver" should "resolve schema for type with no fields" in {
    resolveSchema[Empty] should be(Message())
  }

  it should "resolve schema for type with primitives" in {
    resolveSchema[Primitives] should be(Message(
      Types.primitive(BOOLEAN, REQUIRED).named("boolean"),
      Types.primitive(INT32, REQUIRED).as(OriginalType.INT_32).named("int"),
      Types.primitive(INT64, REQUIRED).as(OriginalType.INT_64).named("long"),
      Types.primitive(FLOAT, REQUIRED).named("float"),
      Types.primitive(DOUBLE, REQUIRED).named("double"),
      Types.primitive(BINARY, OPTIONAL).as(OriginalType.UTF8).named("string"),
      Types.primitive(INT32, REQUIRED).as(OriginalType.INT_16).named("short"),
      Types.primitive(INT32, REQUIRED).as(OriginalType.INT_8).named("byte"),
      Types.primitive(INT32, REQUIRED).as(OriginalType.INT_32).named("char"),
      Types.primitive(FIXED_LEN_BYTE_ARRAY, OPTIONAL).as(OriginalType.DECIMAL)
        .precision(Decimals.Precision)
        .scale(Decimals.Scale)
        .length(Decimals.ByteArrayLength)
        .named("bigDecimal")
    ))
  }

  it should "resolve schema for type containing optional types" in {
    resolveSchema[ContainsOption] should be(Message(
      Types.optional(INT32).as(OriginalType.INT_32).named("optional")
    ))
  }

  it should "resolve schema for type containing collections of primitives" in {
    val elementType = Types.required(INT32).as(OriginalType.INT_32).named(ListSchemaDef.ElementName)
    resolveSchema[Collections] should be(Message(
      Types.optionalList.element(elementType).named("list"),
      Types.optionalList.element(elementType).named("seq"),
      Types.optionalList.element(elementType).named("vector"),
      Types.optionalList.element(elementType).named("set"),
      Types.optionalList.element(elementType).named("array")
    ))
  }

  it should "resolve schema for type containing collection of optional primitives" in {
    resolveSchema[ContainsCollectionOfOptionalPrimitives] should be(Message(
      Types.optionalList
        .element(Types.optional(INT32).as(OriginalType.INT_32).named(ListSchemaDef.ElementName))
        .named("list")
    ))
  }

  it should "resolve schema for type containing collection of collections of primitives" in {
    resolveSchema[ContainsCollectionOfCollections] should be(Message(
      Types.optionalList
        .element(
          Types.optionalList
            .element(Types.required(INT32).as(OriginalType.INT_32)
            .named(ListSchemaDef.ElementName)
        ).named(ListSchemaDef.ElementName)
      ).named("listOfSets")
    ))
  }

  it should "resolve schema for type containing map of primitives" in {
    resolveSchema[ContainsMapOfPrimitives] should be(Message(
      Types.optionalMap()
        .key(Types.required(BINARY).as(OriginalType.UTF8).named(MapSchemaDef.KeyName))
        .value(Types.required(INT32).as(OriginalType.INT_32).named(MapSchemaDef.ValueName))
        .named("map")
    ))
  }

  it should "resolve schema for type containing map of optional primitives" in {
    resolveSchema[ContainsMapOfOptionalPrimitives] should be(Message(
      Types.optionalMap()
        .key(Types.required(BINARY).as(OriginalType.UTF8).named(MapSchemaDef.KeyName))
        .value(Types.optional(INT32).as(OriginalType.INT_32).named(MapSchemaDef.ValueName))
        .named("map")
    ))
  }

  it should "resolve schema for type containing map of collections of primitives" in {
    resolveSchema[ContainsMapOfCollectionsOfPrimitives] should be(Message(
      Types.optionalMap()
        .key(Types.required(BINARY).as(OriginalType.UTF8).named(MapSchemaDef.KeyName))
        .value(
          Types.optionalList
            .element(Types.required(INT32).as(OriginalType.INT_32).named(ListSchemaDef.ElementName))
            .named(MapSchemaDef.ValueName)
        ).named("map")
    ))
  }

  it should "resolve schema for type containing nested class" in {
    resolveSchema[ContainsNestedClass] should be(Message(
      Types.optionalGroup()
        .addField(Types.required(INT32).as(OriginalType.INT_32).named("int"))
        .named("nested")
    ))
  }
  
  it should "resolve schema for type containing optional nested class" in {
    resolveSchema[ContainsOptionalNestedClass] should be(Message(
      Types.optionalGroup()
        .addField(Types.required(INT32).as(OriginalType.INT_32).named("int"))
        .named("nestedOptional")
    ))
  }

  it should "resolve schema for type containing collection of nested class" in {
    val elementType = Types.optionalGroup()
      .addField(Types.required(INT32).as(OriginalType.INT_32).named("int"))
      .named(ListSchemaDef.ElementName)
    resolveSchema[CollectionsOfNestedClass] should be(Message(
      Types.optionalList.element(elementType).named("list"),
      Types.optionalList.element(elementType).named("seq"),
      Types.optionalList.element(elementType).named("vector"),
      Types.optionalList.element(elementType).named("set"),
      Types.optionalList.element(elementType).named("array")
    ))
  }

  it should "resolve schema for type containing map with nested class as value" in {
    resolveSchema[ContainsMapOfNestedClassAsValue] should be(Message(
      Types.optionalMap()
        .key(Types.required(BINARY).as(OriginalType.UTF8).named(MapSchemaDef.KeyName))
        .value(
          Types.optionalGroup()
            .addField(Types.required(INT32).as(OriginalType.INT_32).named("int"))
            .named(MapSchemaDef.ValueName)
        ).named("nested")
    ))
  }

  it should "resolve schema for type containing map with optional nested class as value" in {
    resolveSchema[ContainsMapOfOptionalNestedClassAsValue] should be(Message(
      Types.optionalMap()
        .key(Types.required(BINARY).as(OriginalType.UTF8).named(MapSchemaDef.KeyName))
        .value(
          Types.optionalGroup()
            .addField(Types.required(INT32).as(OriginalType.INT_32).named("int"))
            .named(MapSchemaDef.ValueName)
        ).named("nested")
    ))
  }

  it should "resolve schema for type containing map with collection of nested classes as value" in {
    resolveSchema[ContainsMapOfCollectionsOfNestedClassAsValue] should be(Message(
      Types.optionalMap()
        .key(Types.required(BINARY).as(OriginalType.UTF8).named(MapSchemaDef.KeyName))
        .value(
          Types.optionalList()
            .element(
              Types.optionalGroup()
                .addField(Types.required(INT32).as(OriginalType.INT_32).named("int"))
                .named(ListSchemaDef.ElementName)
            ).named(MapSchemaDef.ValueName)
        ).named("nested")
    ))
  }

}
