package com.github.mjakubowski84.parquet4s

import com.github.mjakubowski84.parquet4s.ScalaPBImplicits.*
import org.apache.hadoop.conf.Configuration
import org.apache.parquet.proto.{ProtoParquetReader, ProtoParquetWriter, ProtoReadSupport, ProtoWriteSupport}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import com.github.mjakubowski84.parquet4s.DataOuterClass.Data as JData

import TestData.*

class Parquet4sScalaPBCoreSpec extends AnyFlatSpec with Matchers {

  "core module" should "be able to read data written with parquet-protobuf" in {
    val outFile    = InMemoryOutputFile(initBufferSize = 4800)
    val hadoopConf = new Configuration()
    hadoopConf.setBoolean(ProtoWriteSupport.PB_SPECS_COMPLIANT_WRITE, true)

    ParquetWriter
      .custom[JData, ProtoParquetWriter.Builder[JData]](
        ProtoParquetWriter.builder[JData](outFile).withMessage(classOf[JData])
      )
      .options(ParquetWriter.Options(hadoopConf = hadoopConf))
      .writeAndClose(javaData)

    ParquetReader.as[Data].read(outFile.toInputFile).toSeq should be(scalaData)
  }

  it should "write data compliant with parquet-protobuf" in {
    val outFile    = InMemoryOutputFile(initBufferSize = 4800)
    val hadoopConf = new Configuration()
    hadoopConf.setClass(ProtoReadSupport.PB_CLASS, classOf[JData], classOf[com.google.protobuf.GeneratedMessageV3])

    ParquetWriter.of[Data].writeAndClose(outFile, scalaData)

    ParquetReader
      .custom[JData.Builder](ProtoParquetReader.builder[JData.Builder](outFile.toInputFile))
      .options(ParquetReader.Options(hadoopConf = hadoopConf))
      .read
      .map(_.build()) should be(javaData)
  }
}
