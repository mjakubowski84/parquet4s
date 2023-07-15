package com.github.mjakubowski84.parquet4s

import akka.actor.ActorSystem
import akka.stream.scaladsl.{Sink, Source}
import akka.testkit.TestKit
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.flatspec.AnyFlatSpecLike
import org.scalatest.matchers.should.Matchers
import org.scalatest.time.{Millis, Seconds, Span}
import com.github.mjakubowski84.parquet4s.DataOuterClass.Data as JData
import com.github.mjakubowski84.parquet4s.ScalaPBImplicits.*
import org.apache.hadoop.conf.Configuration
import org.apache.parquet.proto.{ProtoParquetWriter, ProtoWriteSupport}

import java.nio.file.Files
import scala.jdk.CollectionConverters.*

class Parquet4sScalaPBSpec
    extends TestKit(ActorSystem("parquet4s-scalapb"))
    with AnyFlatSpecLike
    with Matchers
    with ScalaFutures {
  implicit override val patienceConfig = PatienceConfig(timeout = Span(3, Seconds), interval = Span(50, Millis))

  def testWithData(newData: Int => Data): Unit = {
    val data   = (1 to 100).map(newData)
    val tmpDir = Path(Files.createTempDirectory("example"))
    val file   = tmpDir.append("data.parquet")
    val sink   = ParquetStreams.toParquetSingleFile.of[Data].write(file)
    Source(data).runWith(sink).futureValue
    ParquetStreams.fromParquet.as[Data].read(file).runWith(Sink.seq).futureValue shouldBe data
  }

  "parquet4s-scalapb" should "work with primitive types" in {
    testWithData(i => Data(bool = i % 2 == 0))
    testWithData(i => Data(int = i))
    testWithData(i => Data(long = i.toLong))
    testWithData(i => Data(float = i.toFloat))
    testWithData(i => Data(double = i.toDouble))
    testWithData(i => Data(text = i.toString))
    testWithData(i => Data(abc = Data.ABC.fromValue(i % 3)))
  }

  it should "work with message types" in {
    testWithData(i => Data(inner = Some(Data.Inner(i.toString))))
  }

  it should "work with unrecognized enum values" in {
    testWithData(i => Data(abc = Data.ABC.fromValue(i % 5)))
  }

  it should "work with map types" in {
    testWithData(i => Data(map = Map("original" -> i, "doubled" -> 2 * i)))
    testWithData(i => Data(enumMap = Map(i -> Data.ABC.fromValue(i % 5))))
    testWithData(i => Data(msgMap = Map(i.toLong -> Data.Inner(text = "level1"))))
  }

  it should "work with list types" in {
    testWithData(i => Data(boolList = (i to i + 100).map(_ % 2 == 0)))
    testWithData(i => Data(intList = i to i + 100))
    testWithData(i => Data(longList = (i to i + 100).map(_.toLong)))
    testWithData(i => Data(floatList = (i to i + 100).map(_.toFloat)))
    testWithData(i => Data(doubleList = (i to i + 100).map(_.toDouble)))
    testWithData(i => Data(textList = (i to i + 100).map(_.toString)))
    testWithData(i => Data(enumList = (i to i + 100).map(Data.ABC.fromValue)))
    testWithData(i => Data(msgList = (i to i + 100).map(i => Data.Inner(i.toString))))
  }

  it should "be compatible with parquet-protobuf" in {
    val javaData = (1 to 100).map(i =>
      JData
        .newBuilder()
        .setBool(i % 2 == 0)
        .setInt(i)
        .setLong(i.toLong)
        .setFloat(i.toFloat)
        .setDouble(i.toDouble)
        .setText(i.toString)
        .setAbcValue(i % JData.ABC.values().length)
        .setInner(JData.Inner.newBuilder().setText(i.toString))
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
    val scalaData = javaData.map(d => Data.parseFrom(d.toByteArray))

    val tmpDir = Path(Files.createTempDirectory("example"))
    val file   = tmpDir.append("data.parquet")

    val builder    = ProtoParquetWriter.builder[JData](file.hadoopPath).withMessage(classOf[JData])
    val hadoopConf = new Configuration()
    hadoopConf.setBoolean(ProtoWriteSupport.PB_SPECS_COMPLIANT_WRITE, true)
    val sink = ParquetStreams.toParquetSingleFile
      .custom[JData, ProtoParquetWriter.Builder[JData]](builder)
      .options(ParquetWriter.Options(hadoopConf = hadoopConf))
      .write
    Source(javaData).runWith(sink).futureValue

    ParquetStreams.fromParquet.as[Data].read(file).runWith(Sink.seq).futureValue shouldBe scalaData
  }
}
