package com.github.mjakubowski84.parquet4s

import com.github.mjakubowski84.parquet4s.ScalaPBImplicits._
import org.apache.parquet.schema.{Type, Types}
import scalapb.{GeneratedMessage, GeneratedMessageCompanion}

import scala.jdk.CollectionConverters.*

class ScalaPBParquetSchemaResolver[T <: GeneratedMessage: GeneratedMessageCompanion] extends ParquetSchemaResolver[T] {
  private val cmp = implicitly[GeneratedMessageCompanion[T]]

  override def schemaName: Option[String] = Option(cmp.scalaDescriptor.name)

  override def resolveSchema(cursor: Cursor): List[Type] = {
    val md = cmp.scalaDescriptor
    Types
      .buildMessage()
      .addFields(md.fields)
      .named(md.fullName)
      .getFields
      .iterator()
      .asScala
      .toList
  }
}
