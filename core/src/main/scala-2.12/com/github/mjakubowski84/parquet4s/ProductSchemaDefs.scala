package com.github.mjakubowski84.parquet4s

import shapeless.LowPriority

import scala.annotation.nowarn

trait ProductSchemaDefs {
  implicit def productSchema[T](implicit
      @nowarn ev: LowPriority,
      parquetSchemaResolver: ParquetSchemaResolver[T]
  ): TypedSchemaDef[T] =
    SchemaDef.group(parquetSchemaResolver.resolveSchema(Cursor.simple)*).withMetadata(SchemaDef.Meta.Generated).typed[T]

}
