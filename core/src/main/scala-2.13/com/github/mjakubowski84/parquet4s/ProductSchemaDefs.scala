package com.github.mjakubowski84.parquet4s

import shapeless.LowPriority

trait ProductSchemaDefs {
  implicit def productSchema[T](implicit
      ev: LowPriority,
      parquetSchemaResolver: ParquetSchemaResolver[T]
  ): TypedSchemaDef[T] =
    SchemaDef
      .group(parquetSchemaResolver.resolveSchema(Cursor.simple)*)
      .withMetadata(SchemaDef.Meta.Generated)
      .typed[T]

}
