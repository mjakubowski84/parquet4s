package com.github.mjakubowski84.parquet4s

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
    def valueOf(name: String): Type = values
      .find(_.toString == name)
      .getOrElse(throw new IllegalArgumentException(s"Invalid dict name: $name"))

    def random: Type = values(Random.nextInt(values.length))

    // required for reading
    implicit val decoder: OptionalValueDecoder[Type] =
      (value: Value, _: ValueCodecConfiguration) =>
        value match {
          case BinaryValue(binary) => valueOf(binary.toStringUsingUTF8)
        }
    // required for writing
    implicit val encoder: OptionalValueEncoder[Type] =
      (data: Type, _: ValueCodecConfiguration) => BinaryValue(data.toString)
    // required for writing
    implicit val schema: TypedSchemaDef[Type] =
      SchemaDef
        .primitive(
          primitiveType         = PrimitiveType.PrimitiveTypeName.BINARY,
          required              = false,
          logicalTypeAnnotation = Option(LogicalTypeAnnotation.stringType())
        )
        .typed[Type]
  }

}
