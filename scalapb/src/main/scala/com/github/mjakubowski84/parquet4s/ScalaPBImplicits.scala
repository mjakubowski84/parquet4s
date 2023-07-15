package com.github.mjakubowski84.parquet4s

import scalapb.descriptors.{Descriptor, FieldDescriptor, ScalaType}
import scalapb.{GeneratedMessage, GeneratedMessageCompanion}
import org.apache.parquet.schema.LogicalTypeAnnotation.{enumType, listType, mapType, stringType}
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.*
import org.apache.parquet.schema.Types.{Builder, GroupBuilder}
import org.apache.parquet.schema.{PrimitiveType, Type, Types}

import scala.util.matching.Regex

object ScalaPBImplicits {
  implicit def scalapbParquetRecordEncoder[T <: GeneratedMessage]: ParquetRecordEncoder[T] =
    new ScalaPBParquetRecordEncoder[T]

  implicit def scalaPBParquetRecordDecoder[T <: GeneratedMessage: GeneratedMessageCompanion]: ParquetRecordDecoder[T] =
    new ScalaPBParquetRecordDecoder[T]

  implicit def scalapbParquetSchemaResolver[T <: GeneratedMessage: GeneratedMessageCompanion]
      : ParquetSchemaResolver[T] =
    new ScalaPBParquetSchemaResolver[T]

  private[parquet4s] val MetadataEnumPrefix: String  = "parquet.proto.enum."
  private[parquet4s] val EnumNameNumberPairRe: Regex = "(.*?):(.*?)(?:,|$)".r

  implicit private[parquet4s] class RichGroupBuilder[T](private val builder: GroupBuilder[T]) extends AnyVal {
    def addField(fd: FieldDescriptor): Builder[_ <: Builder[_, GroupBuilder[T]], GroupBuilder[T]] =
      fd.scalaType match {
        case ScalaType.Message(md) =>
          if (fd.isMapField) addMapField(md.fields)
          else if (fd.isRepeated) addRepeatedMessage(md)
          else builder.group(repetition(fd)).addFields(md.fields)
        case _ =>
          val rawType = primitiveType(fd)
          if (fd.isRepeated) addRepeatedPrimitive(rawType)
          else builder.primitive(rawType.getPrimitiveTypeName, repetition(fd)).as(rawType.getLogicalTypeAnnotation)
      }

    def addMapField(fields: Vector[FieldDescriptor]): GroupBuilder[GroupBuilder[T]] = {
      val keyFd   = fields.head
      val valFd   = fields.tail.head
      val keyType = primitiveType(keyFd).asPrimitiveType()
      builder
        .optionalGroup()
        .as(mapType())
        .addMapKey(keyType)
        .addField(valFd)
        .named("value")
        .named("key_value")
    }

    def addMapKey(keyType: PrimitiveType): GroupBuilder[GroupBuilder[T]] =
      builder
        .repeatedGroup()
        .primitive(keyType.getPrimitiveTypeName, Type.Repetition.REQUIRED)
        .as(keyType.getLogicalTypeAnnotation)
        .named("key")

    def addRepeatedMessage(fd: Descriptor): GroupBuilder[GroupBuilder[T]] =
      builder
        .optionalGroup()
        .as(listType())
        .repeatedGroup()
        .optionalGroup()
        .addFields(fd.fields)
        .named("element")
        .named("list")

    def addRepeatedPrimitive(elementType: PrimitiveType) =
      builder
        .optionalGroup()
        .as(listType())
        .repeatedGroup()
        .primitive(elementType.getPrimitiveTypeName, Type.Repetition.REQUIRED)
        .as(elementType.getLogicalTypeAnnotation)
        .named("element")
        .named("list")

    def addFields(fields: Vector[FieldDescriptor]): GroupBuilder[T] =
      fields.foldLeft(builder)((builder, fd) => builder.addField(fd).id(fd.index).named(fd.name))

    def primitiveType(fd: FieldDescriptor): PrimitiveType = {
      val rep = repetition(fd)
      fd.scalaType match {
        case ScalaType.Boolean    => Types.primitive(BOOLEAN, rep).named(fd.name)
        case ScalaType.Enum(_)    => Types.primitive(BINARY, rep).as(enumType()).named(fd.name)
        case ScalaType.Int        => Types.primitive(INT32, rep).named(fd.name)
        case ScalaType.Long       => Types.primitive(INT64, rep).named(fd.name)
        case ScalaType.Float      => Types.primitive(FLOAT, rep).named(fd.name)
        case ScalaType.Double     => Types.primitive(DOUBLE, rep).named(fd.name)
        case ScalaType.String     => Types.primitive(BINARY, rep).as(stringType()).named(fd.name)
        case ScalaType.ByteString => Types.primitive(BINARY, rep).named(fd.name)
        case ScalaType.Message(_) => throw new IllegalArgumentException(s"Field is not primitive type: ${fd.fullName}")
      }
    }

    def repetition(fd: FieldDescriptor): Type.Repetition =
      if (fd.isRequired) Type.Repetition.REQUIRED
      else if (fd.isRepeated) Type.Repetition.REPEATED
      else Type.Repetition.OPTIONAL;
  }
}
