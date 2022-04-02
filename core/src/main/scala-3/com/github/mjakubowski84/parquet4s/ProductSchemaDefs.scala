package com.github.mjakubowski84.parquet4s

import scala.reflect.{ClassTag, classTag}
import scala.util.NotGiven

trait ProductSchemaDefs:

  given productSchema[T <: Product: ParquetSchemaResolver](using
      NotGiven[TypedSchemaDef[T]]
  ): TypedSchemaDef[T] =
    SchemaDef
      .group(summon[ParquetSchemaResolver[T]].resolveSchema(Cursor.simple)*)
      .withMetadata(SchemaDef.Meta.Generated)
      .typed[T]

end ProductSchemaDefs
