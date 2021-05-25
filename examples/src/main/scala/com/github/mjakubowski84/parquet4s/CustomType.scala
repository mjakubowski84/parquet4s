package com.github.mjakubowski84.parquet4s

import com.github.mjakubowski84.parquet4s.ParquetSchemaResolver.TypedSchemaDef
import org.apache.parquet.schema.{LogicalTypeAnnotation, PrimitiveType}

import scala.util.Random

object CustomType {

  object Dict {

    sealed trait Type
    case object A extends Type
    case object B extends Type
    case object C extends Type
    case object D extends Type

    val values: List[Type] = List(A, B, C, D)
    def valueOf(name: String): Type = values.find(_.toString == name)
      .getOrElse(throw new IllegalArgumentException(s"Invalid dict name: $name"))

    def random: Type = values(Random.nextInt(values.length))

    // required for reading and writing
    implicit val codec: OptionalValueCodec[Type] = new OptionalValueCodec[Type] {
      override protected def decodeNonNull(value: Value, configuration: ValueCodecConfiguration): Type = value match {
        case BinaryValue(binary) => valueOf(binary.toStringUsingUTF8)
      }
      override protected def encodeNonNull(data: Type, configuration: ValueCodecConfiguration): Value =
        BinaryValue(data.toString)
    }

    // required for writing only
    implicit val schema: TypedSchemaDef[Type] =
        SchemaDef.primitive(
          primitiveType = PrimitiveType.PrimitiveTypeName.BINARY,
          required = false,
          logicalTypeAnnotation = Option(LogicalTypeAnnotation.stringType())
        ).typed[Type]
  }

}
