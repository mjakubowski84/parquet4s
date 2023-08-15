package com.github.mjakubowski84.parquet4s

import com.github.mjakubowski84.parquet4s.DataOuterClass.Data as JData
import scala.jdk.CollectionConverters.*

object TestData {

  val javaData: Seq[JData] = (1 to 100)
    .map(i =>
      JData
        .newBuilder()
        .setBool(i % 2 == 0)
        .setInt(i)
        .setLong(i.toLong)
        .setFloat(i.toFloat)
        .setDouble(i.toDouble)
        .setText(i.toString)
        .setAbcValue(i % JData.ABC.values().length)
        .setInner(JData.Inner.newBuilder().setText(i.toString).build())
        .addAllBoolList((i to i + 100).map(_ % 2 == 0).map(java.lang.Boolean.valueOf).asJava)
        .addAllIntList((i to i + 100).map(Integer.valueOf).asJava)
        .addAllLongList((i to i + 100).map(_.toLong).map(java.lang.Long.valueOf).asJava)
        .addAllFloatList((i to i + 100).map(_.toFloat).map(java.lang.Float.valueOf).asJava)
        .addAllDoubleList((i to i + 100).map(_.toDouble).map(java.lang.Double.valueOf).asJava)
        .addAllTextList((i to i + 100).map(_.toString).asJava)
        .addAllEnumListValue((i to i + 100).map(_ % JData.ABC.values().length).map(Integer.valueOf).asJava)
        .addAllMsgList((i to i + 100).map(i => JData.Inner.newBuilder().setText(i.toString).build()).asJava)
        .build()
    )
  val scalaData: Seq[Data] = javaData.map(d => Data.parseFrom(d.toByteArray))

}
