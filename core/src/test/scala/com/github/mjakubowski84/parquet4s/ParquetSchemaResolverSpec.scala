package com.github.mjakubowski84.parquet4s

import com.github.mjakubowski84.parquet4s.ParquetSchemaResolver.resolveSchema
import com.github.mjakubowski84.parquet4s.TestCases._
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName._
import org.apache.parquet.schema.Type.Repetition._
import org.apache.parquet.schema.{OriginalType, PrimitiveType, Types}
import org.scalatest.{FlatSpec, Matchers}


class ParquetSchemaResolverSpec extends FlatSpec with Matchers {

  "ParquetSchemaResolver" should "resolve schema for type with no fields" in {
    resolveSchema[Empty] should be(Message(classOf[Empty].getCanonicalName))
  }

  it should "resolve schema for type with primitives" in {
    resolveSchema[Primitives] should be(Message(classOf[Primitives].getCanonicalName,
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
    resolveSchema[ContainsOption] should be(Message(classOf[ContainsOption].getCanonicalName,
      Types.optional(INT32).as(OriginalType.INT_32).named("optional")
    ))
  }

  it should "resolve schema for type containing collections of primitives" in {
    val elementType = Types.required(INT32).as(OriginalType.INT_32).named(ListSchemaDef.ElementName)
    resolveSchema[Collections] should be(Message(classOf[Collections].getCanonicalName,
      Types.optionalList.element(elementType).named("list"),
      Types.optionalList.element(elementType).named("seq"),
      Types.optionalList.element(elementType).named("vector"),
      Types.optionalList.element(elementType).named("set"),
      Types.optionalList.element(elementType).named("array")
    ))
  }

  it should "resolve schema for type containing collection of optional primitives" in {
    resolveSchema[ContainsCollectionOfOptionalPrimitives] should be(Message(classOf[ContainsCollectionOfOptionalPrimitives].getCanonicalName,
      Types.optionalList
        .element(Types.optional(INT32).as(OriginalType.INT_32).named(ListSchemaDef.ElementName))
        .named("list")
    ))
  }

  it should "resolve schema for type containing collection of collections of primitives" in {
    resolveSchema[ContainsCollectionOfCollections] should be(Message(classOf[ContainsCollectionOfCollections].getCanonicalName,
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
    resolveSchema[ContainsMapOfPrimitives] should be(Message(classOf[ContainsMapOfPrimitives].getCanonicalName,
      Types.optionalMap()
        .key(Types.required(BINARY).as(OriginalType.UTF8).named(MapSchemaDef.KeyName))
        .value(Types.required(INT32).as(OriginalType.INT_32).named(MapSchemaDef.ValueName))
        .named("map")
    ))
  }

  it should "resolve schema for type containing map of optional primitives" in {
    resolveSchema[ContainsMapOfOptionalPrimitives] should be(Message(classOf[ContainsMapOfOptionalPrimitives].getCanonicalName,
      Types.optionalMap()
        .key(Types.required(BINARY).as(OriginalType.UTF8).named(MapSchemaDef.KeyName))
        .value(Types.optional(INT32).as(OriginalType.INT_32).named(MapSchemaDef.ValueName))
        .named("map")
    ))
  }

  it should "resolve schema for type containing map of collections of primitives" in {
    resolveSchema[ContainsMapOfCollectionsOfPrimitives] should be(Message(classOf[ContainsMapOfCollectionsOfPrimitives].getCanonicalName,
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
    resolveSchema[ContainsNestedClass] should be(Message(classOf[ContainsNestedClass].getCanonicalName,
      Types.optionalGroup()
        .addField(Types.required(INT32).as(OriginalType.INT_32).named("int"))
        .named("nested")
    ))
  }
  
  it should "resolve schema for type containing optional nested class" in {
    resolveSchema[ContainsOptionalNestedClass] should be(Message(classOf[ContainsOptionalNestedClass].getCanonicalName,
      Types.optionalGroup()
        .addField(Types.required(INT32).as(OriginalType.INT_32).named("int"))
        .named("nestedOptional")
    ))
  }

  it should "resolve schema for type containing collection of nested class" in {
    val elementType = Types.optionalGroup()
      .addField(Types.required(INT32).as(OriginalType.INT_32).named("int"))
      .named(ListSchemaDef.ElementName)
    resolveSchema[CollectionsOfNestedClass] should be(Message(classOf[CollectionsOfNestedClass].getCanonicalName,
      Types.optionalList.element(elementType).named("list"),
      Types.optionalList.element(elementType).named("seq"),
      Types.optionalList.element(elementType).named("vector"),
      Types.optionalList.element(elementType).named("set"),
      Types.optionalList.element(elementType).named("array")
    ))
  }

  it should "resolve schema for type containing map with nested class as value" in {
    resolveSchema[ContainsMapOfNestedClassAsValue] should be(Message(classOf[ContainsMapOfNestedClassAsValue].getCanonicalName,
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
    resolveSchema[ContainsMapOfOptionalNestedClassAsValue] should be(Message(classOf[ContainsMapOfOptionalNestedClassAsValue].getCanonicalName,
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
    resolveSchema[ContainsMapOfCollectionsOfNestedClassAsValue] should be(Message(classOf[ContainsMapOfCollectionsOfNestedClassAsValue].getCanonicalName,
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
