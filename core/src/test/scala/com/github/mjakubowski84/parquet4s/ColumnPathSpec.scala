package com.github.mjakubowski84.parquet4s

import com.github.mjakubowski84.parquet4s.LogicalTypes.StringType
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.BINARY
import org.apache.parquet.schema.Type.Repetition.OPTIONAL
import org.apache.parquet.schema.Types
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class ColumnPathSpec extends AnyFlatSpec with Matchers {

  "ColumnPath" should "be created with proper elements" in {
    Col("path").elements should be(Seq("path"))
    Col("path.subPath").elements should be(Seq("path", "subPath"))
  }

  it should "be appendable" in {
    Col("path").appendElement("subPath").elements should be(Seq("path", "subPath"))
  }

  it should "turn to dot path" in {
    Col("path").toString should be("path")
    Col("path.subPath").toString should be("path.subPath")
  }

  it should "be able to turn to typed" in {
    Col("path").as[String].toType should be(Types.primitive(BINARY, OPTIONAL).as(StringType).named("path"))
  }

}
