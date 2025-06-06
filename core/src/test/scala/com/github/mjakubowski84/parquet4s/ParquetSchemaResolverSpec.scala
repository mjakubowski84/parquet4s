package com.github.mjakubowski84.parquet4s

import com.github.mjakubowski84.parquet4s.LogicalTypes.*
import com.github.mjakubowski84.parquet4s.ParquetSchemaResolver.{resolveSchema, findType}
import com.github.mjakubowski84.parquet4s.TestCases.*
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.*
import org.apache.parquet.schema.Type.Repetition.*
import org.apache.parquet.schema.Types
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

object ParquetSchemaResolverSpec {
  object Malformed {
    case class Clazz()
  }
}

class ParquetSchemaResolverSpec extends AnyFlatSpec with Matchers {

  "ParquetSchemaResolver" should "resolve schema for type with no fields" in {
    val resolved = resolveSchema[Empty]
    resolved should be(Message(Some(classOf[Empty].getCanonicalName)))
  }

  it should "resolve schema for type with primitives" in {
    val resolved = resolveSchema[Primitives]
    resolved should be(
      Message(
        Some(classOf[Primitives].getCanonicalName),
        Types.primitive(BOOLEAN, REQUIRED).named("boolean"),
        Types.primitive(INT32, REQUIRED).as(Int32Type).named("int"),
        Types.primitive(INT64, REQUIRED).as(Int64Type).named("long"),
        Types.primitive(FLOAT, REQUIRED).named("float"),
        Types.primitive(DOUBLE, REQUIRED).named("double"),
        Types.primitive(BINARY, OPTIONAL).as(StringType).named("string"),
        Types.primitive(INT32, REQUIRED).as(Int16Type).named("short"),
        Types.primitive(INT32, REQUIRED).as(Int8Type).named("byte"),
        Types.primitive(INT32, REQUIRED).as(Int32Type).named("char"),
        Types
          .primitive(FIXED_LEN_BYTE_ARRAY, OPTIONAL)
          .as(DefaultDecimalType)
          .length(DecimalFormat.Default.byteArrayLength)
          .named("bigDecimal")
      )
    )
  }

  it should "resolve schema for type containing optional types" in {
    val resolved = resolveSchema[ContainsOption]
    resolved should be(
      Message(
        Some(classOf[ContainsOption].getCanonicalName),
        Types.optional(INT32).as(Int32Type).named("optional")
      )
    )
  }

  it should "resolve schema for type containing collections of primitives" in {
    val elementType = Types.required(INT32).as(Int32Type).named(ListSchemaDef.ElementName)
    val resolved    = resolveSchema[Collections]
    resolved should be(
      Message(
        Some(classOf[Collections].getCanonicalName),
        Types.optionalList.element(elementType).named("list"),
        Types.optionalList.element(elementType).named("seq"),
        Types.optionalList.element(elementType).named("vector"),
        Types.optionalList.element(elementType).named("set"),
        Types.optionalList.element(elementType).named("array")
      )
    )
  }

  it should "resolve schema for type containing collection of optional primitives" in {
    val resolved = resolveSchema[ContainsCollectionOfOptionalPrimitives]
    resolved should be(
      Message(
        Some(classOf[ContainsCollectionOfOptionalPrimitives].getCanonicalName),
        Types.optionalList
          .element(Types.optional(INT32).as(Int32Type).named(ListSchemaDef.ElementName))
          .named("list")
      )
    )
  }

  it should "resolve schema for type containing collection of collections of primitives" in {
    val resolved = resolveSchema[ContainsCollectionOfCollections]
    resolved should be(
      Message(
        Some(classOf[ContainsCollectionOfCollections].getCanonicalName),
        Types.optionalList
          .element(
            Types.optionalList
              .element(
                Types
                  .required(INT32)
                  .as(Int32Type)
                  .named(ListSchemaDef.ElementName)
              )
              .named(ListSchemaDef.ElementName)
          )
          .named("listOfSets")
      )
    )
  }

  it should "resolve schema for type containing map of primitives" in {
    val resolved = resolveSchema[ContainsMapOfPrimitives]
    resolved should be(
      Message(
        Some(classOf[ContainsMapOfPrimitives].getCanonicalName),
        Types
          .optionalMap()
          .key(Types.required(BINARY).as(StringType).named(MapSchemaDef.KeyName))
          .value(Types.required(INT32).as(Int32Type).named(MapSchemaDef.ValueName))
          .named("map")
      )
    )
  }

  it should "resolve schema for type containing map of optional primitives" in {
    val resolved = resolveSchema[ContainsMapOfOptionalPrimitives]
    resolved should be(
      Message(
        Some(classOf[ContainsMapOfOptionalPrimitives].getCanonicalName),
        Types
          .optionalMap()
          .key(Types.required(BINARY).as(StringType).named(MapSchemaDef.KeyName))
          .value(Types.optional(INT32).as(Int32Type).named(MapSchemaDef.ValueName))
          .named("map")
      )
    )
  }

  it should "resolve schema for type containing map of collections of primitives" in {
    val resolved = resolveSchema[ContainsMapOfCollectionsOfPrimitives]
    resolved should be(
      Message(
        Some(classOf[ContainsMapOfCollectionsOfPrimitives].getCanonicalName),
        Types
          .optionalMap()
          .key(Types.required(BINARY).as(StringType).named(MapSchemaDef.KeyName))
          .value(
            Types.optionalList
              .element(Types.required(INT32).as(Int32Type).named(ListSchemaDef.ElementName))
              .named(MapSchemaDef.ValueName)
          )
          .named("map")
      )
    )
  }

  it should "resolve schema for type containing nested class" in {
    val resolved = resolveSchema[ContainsNestedClass]
    resolved should be(
      Message(
        Some(classOf[ContainsNestedClass].getCanonicalName),
        Types
          .optionalGroup()
          .addField(Types.required(INT32).as(Int32Type).named("int"))
          .named("nested")
      )
    )
  }

  it should "resolve schema for type containing optional nested class" in {
    val resolved = resolveSchema[ContainsOptionalNestedClass]
    resolved should be(
      Message(
        Some(classOf[ContainsOptionalNestedClass].getCanonicalName),
        Types
          .optionalGroup()
          .addField(Types.required(INT32).as(Int32Type).named("int"))
          .named("nestedOptional")
      )
    )
  }

  it should "resolve schema for type containing collection of nested class" in {
    val elementType = Types
      .optionalGroup()
      .addField(Types.required(INT32).as(Int32Type).named("int"))
      .named(ListSchemaDef.ElementName)
    val resolved = resolveSchema[CollectionsOfNestedClass]
    resolved should be(
      Message(
        Some(classOf[CollectionsOfNestedClass].getCanonicalName),
        Types.optionalList.element(elementType).named("list"),
        Types.optionalList.element(elementType).named("seq"),
        Types.optionalList.element(elementType).named("vector"),
        Types.optionalList.element(elementType).named("set"),
        Types.optionalList.element(elementType).named("array")
      )
    )
  }

  it should "resolve schema for type containing map with nested class as value" in {
    val resolved = resolveSchema[ContainsMapOfNestedClassAsValue]
    resolved should be(
      Message(
        Some(classOf[ContainsMapOfNestedClassAsValue].getCanonicalName),
        Types
          .optionalMap()
          .key(Types.required(BINARY).as(StringType).named(MapSchemaDef.KeyName))
          .value(
            Types
              .optionalGroup()
              .addField(Types.required(INT32).as(Int32Type).named("int"))
              .named(MapSchemaDef.ValueName)
          )
          .named("nested")
      )
    )
  }

  it should "resolve schema for type containing map with optional nested class as value" in {
    val resolved = resolveSchema[ContainsMapOfOptionalNestedClassAsValue]
    resolved should be(
      Message(
        Some(classOf[ContainsMapOfOptionalNestedClassAsValue].getCanonicalName),
        Types
          .optionalMap()
          .key(Types.required(BINARY).as(StringType).named(MapSchemaDef.KeyName))
          .value(
            Types
              .optionalGroup()
              .addField(Types.required(INT32).as(Int32Type).named("int"))
              .named(MapSchemaDef.ValueName)
          )
          .named("nested")
      )
    )
  }

  it should "resolve schema for type containing map with collection of nested classes as value" in {
    val resolved = resolveSchema[ContainsMapOfCollectionsOfNestedClassAsValue]
    resolved should be(
      Message(
        Some(classOf[ContainsMapOfCollectionsOfNestedClassAsValue].getCanonicalName),
        Types
          .optionalMap()
          .key(Types.required(BINARY).as(StringType).named(MapSchemaDef.KeyName))
          .value(
            Types
              .optionalList()
              .element(
                Types
                  .optionalGroup()
                  .addField(Types.required(INT32).as(Int32Type).named("int"))
                  .named(ListSchemaDef.ElementName)
              )
              .named(MapSchemaDef.ValueName)
          )
          .named("nested")
      )
    )
  }

  it should "resolve schema name from class name" in {
    resolveSchema[Empty].getName should be("com.github.mjakubowski84.parquet4s.TestCases.Empty")
  }

  it should "use default schema name when fails to resolve a class name" in {
    case class Clazz()
    resolveSchema[Clazz].getName should be(Message.DefaultName)
  }

  ignore should "use default schema name when processing a class that JVM treats as malformed" in {
    val resolved = resolveSchema[ParquetSchemaResolverSpec.Malformed.Clazz]
    resolved.getName should be(Message.DefaultName)
  }

  "findType" should "find a field" in {
    findType[Primitives](Col("int")) should be(Some(Types.primitive(INT32, REQUIRED).as(Int32Type).named("int")))
  }

  it should "process empty class" in {
    findType[Empty](Col("")) should be(None)
    findType[Empty](Col("invalid")) should be(None)
  }

  it should "handle invalid paths" in {
    findType[Primitives](Col("invalid")) should be(None)
    findType[Primitives](Col("")) should be(None)
  }

  it should "find a nested field" in {
    findType[ContainsNestedClass](Col("nested.int")) should be(Some(Types.required(INT32).as(Int32Type).named("int")))
  }

  it should "provide complex type result" in {
    findType[ContainsNestedClass](Col("nested")) should be(
      Some(
        Types
          .optionalGroup()
          .addField(Types.required(INT32).as(Int32Type).named("int"))
          .named("nested")
      )
    )
  }

}
