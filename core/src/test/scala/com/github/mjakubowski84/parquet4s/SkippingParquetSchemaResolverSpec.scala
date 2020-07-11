package com.github.mjakubowski84.parquet4s

import com.github.mjakubowski84.parquet4s.SkippingParquetSchemaResolver.resolveSchema
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.{BINARY, INT32}
import org.apache.parquet.schema.Type.Repetition.{OPTIONAL, REQUIRED}
import org.apache.parquet.schema.{MessageType, OriginalType, Types}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class SkippingParquetSchemaResolverSpec extends AnyFlatSpec with Matchers {

  case class Street(name: String, more: Option[String])
  case class Address(street: Street, city: String)
  case class Person(name: String, age: Int, address: Address)

  implicit val fullSchema: MessageType = Message(
    Some(classOf[Person].getCanonicalName),
    Types.primitive(BINARY, OPTIONAL).as(OriginalType.UTF8).named("name"),
    Types.primitive(INT32, REQUIRED).as(OriginalType.INT_32).named("age"),
    Types.optionalGroup()
      .addField(Types.optionalGroup()
        .addField(Types.primitive(BINARY, OPTIONAL).as(OriginalType.UTF8).named("name"))
        .addField(Types.primitive(BINARY, OPTIONAL).as(OriginalType.UTF8).named("more"))
        .named("street"))
      .addField(Types.primitive(BINARY, OPTIONAL).as(OriginalType.UTF8).named("city"))
      .named("address")
  )

  "SkippingParquetSchemaResolver" should "create unpartitioned schema" in {
    resolveSchema[Person](Set.empty) should be(fullSchema)
  }

  it should "skip a field at a long path" in {
    resolveSchema[Person](Set("address.street.name")) should be(Message(
      Some(classOf[Person].getCanonicalName),
      Types.primitive(BINARY, OPTIONAL).as(OriginalType.UTF8).named("name"),
      Types.primitive(INT32, REQUIRED).as(OriginalType.INT_32).named("age"),
      Types.optionalGroup()
        .addField(Types.optionalGroup()
          .addField(Types.primitive(BINARY, OPTIONAL).as(OriginalType.UTF8).named("more"))
          .named("street"))
        .addField(Types.primitive(BINARY, OPTIONAL).as(OriginalType.UTF8).named("city"))
        .named("address")
    ))
  }

  it should "skip a mid path" in {
    resolveSchema[Person](Set("address.street")) should be(Message(
      Some(classOf[Person].getCanonicalName),
      Types.primitive(BINARY, OPTIONAL).as(OriginalType.UTF8).named("name"),
      Types.primitive(INT32, REQUIRED).as(OriginalType.INT_32).named("age"),
      Types.optionalGroup()
        .addField(Types.primitive(BINARY, OPTIONAL).as(OriginalType.UTF8).named("city"))
        .named("address")
    ))
  }

  it should "skip all fields of case class" in {
    resolveSchema[Person](Set("address.street.name", "address.street.more")) should be(Message(
      Some(classOf[Person].getCanonicalName),
      Types.primitive(BINARY, OPTIONAL).as(OriginalType.UTF8).named("name"),
      Types.primitive(INT32, REQUIRED).as(OriginalType.INT_32).named("age"),
      Types.optionalGroup()
        .addField(Types.primitive(BINARY, OPTIONAL).as(OriginalType.UTF8).named("city"))
        .named("address")
    ))
  }

  it should "skip the only field from a simple case class" in {
    case class Simple(field: String)
    resolveSchema[Simple](Set("field")) should be(Message(None))
  }

  it should "process empty class" in {
    case class Empty()
    resolveSchema[Empty](Set("field")) should be(Message(None))
  }

  "Generic SkippingParquetSchemaResolver" should "create unpartitioned schema" in {
    resolveSchema[RowParquetRecord](Set.empty) should be(fullSchema)
  }

  it should "skip a field at a long path" in {
    resolveSchema[RowParquetRecord](Set("address.street.name")) should be(Message(
      Some(classOf[Person].getCanonicalName),
      Types.primitive(BINARY, OPTIONAL).as(OriginalType.UTF8).named("name"),
      Types.primitive(INT32, REQUIRED).as(OriginalType.INT_32).named("age"),
      Types.optionalGroup()
        .addField(Types.optionalGroup()
          .addField(Types.primitive(BINARY, OPTIONAL).as(OriginalType.UTF8).named("more"))
          .named("street"))
        .addField(Types.primitive(BINARY, OPTIONAL).as(OriginalType.UTF8).named("city"))
        .named("address")
    ))
  }

  it should "skip a mid path" in {
    resolveSchema[RowParquetRecord](Set("address.street")) should be(Message(
      Some(classOf[Person].getCanonicalName),
      Types.primitive(BINARY, OPTIONAL).as(OriginalType.UTF8).named("name"),
      Types.primitive(INT32, REQUIRED).as(OriginalType.INT_32).named("age"),
      Types.optionalGroup()
        .addField(Types.primitive(BINARY, OPTIONAL).as(OriginalType.UTF8).named("city"))
        .named("address")
    ))
  }

  it should "skip all fields of case class" in {
    resolveSchema[RowParquetRecord](Set("address.street.name", "address.street.more")) should be(Message(
      Some(classOf[Person].getCanonicalName),
      Types.primitive(BINARY, OPTIONAL).as(OriginalType.UTF8).named("name"),
      Types.primitive(INT32, REQUIRED).as(OriginalType.INT_32).named("age"),
      Types.optionalGroup()
        .addField(Types.primitive(BINARY, OPTIONAL).as(OriginalType.UTF8).named("city"))
        .named("address")
    ))
  }

  it should "skip the only field from a simple case class" in {
    val simpleMessage: MessageType = Message(
      Some("Simple"),
      Types.primitive(BINARY, OPTIONAL).as(OriginalType.UTF8).named("field")
    )
    implicit val resolver: SkippingParquetSchemaResolver[RowParquetRecord] =
      RowParquetRecord.genericSkippingParquetSchemaResolver(simpleMessage)
    resolveSchema[RowParquetRecord](Set("field")) should be(Message(Some("Simple")))
  }

  it should "process empty class" in {
    implicit val resolver: SkippingParquetSchemaResolver[RowParquetRecord] =
      RowParquetRecord.genericSkippingParquetSchemaResolver(Message(Some("Empty")))
    resolveSchema[RowParquetRecord](Set("field")) should be(Message(Some("Empty")))
  }

}
