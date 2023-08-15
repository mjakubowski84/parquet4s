package com.github.mjakubowski84.parquet4s

import cats.effect.IO
import cats.effect.testing.scalatest.AsyncIOSpec
import com.github.mjakubowski84.parquet4s.DataOuterClass.Data as JData
import com.github.mjakubowski84.parquet4s.ScalaPBImplicits.*
import fs2.Stream
import org.apache.hadoop.conf.Configuration
import org.apache.parquet.proto.{ProtoParquetReader, ProtoParquetWriter, ProtoReadSupport, ProtoWriteSupport}
import org.scalatest.flatspec.AsyncFlatSpec
import org.scalatest.matchers.should.Matchers

import TestData.*

class Parquet4sScalaPBFS2Spec extends AsyncFlatSpec with AsyncIOSpec with Matchers {

  "fs2 module" should "be compatible with parquet-protobuf" in {
    val outFile    = InMemoryOutputFile(initBufferSize = 4800)
    val hadoopConf = new Configuration()
    hadoopConf.setBoolean(ProtoWriteSupport.PB_SPECS_COMPLIANT_WRITE, true)

    def write: Stream[IO, Nothing] =
      Stream
        .iterable(javaData)
        .through(
          parquet
            .writeSingleFile[IO]
            .custom[JData, ProtoParquetWriter.Builder[JData]](
              ProtoParquetWriter.builder[JData](outFile).withMessage(classOf[JData])
            )
            .options(ParquetWriter.Options(hadoopConf = hadoopConf))
            .write
        )

    def read: Stream[IO, Vector[Data]] =
      parquet.fromParquet[IO].as[Data].read(outFile.toInputFile).fold(Vector.empty[Data])(_ :+ _)

    (write ++ read).map(_ should be(scalaData)).compile.lastOrError
  }

  it should "write data compliant with parquet-protobuf" in {
    val outFile    = InMemoryOutputFile(initBufferSize = 4800)
    val hadoopConf = new Configuration()
    hadoopConf.setClass(ProtoReadSupport.PB_CLASS, classOf[JData], classOf[com.google.protobuf.GeneratedMessageV3])

    def write: Stream[IO, Nothing] =
      Stream
        .iterable(scalaData)
        .through(
          parquet
            .writeSingleFile[IO]
            .of[Data]
            .write(outFile)
        )

    def read: Stream[IO, Vector[JData]] =
      parquet
        .fromParquet[IO]
        .custom[JData.Builder](ProtoParquetReader.builder[JData.Builder](outFile.toInputFile))
        .options(ParquetReader.Options(hadoopConf = hadoopConf))
        .chunkSize(chunkSize = 1)
        // due to bug in ProtoParquetReader - the fact that read elements are unsafe instances of JData.Builder
        // we must limit chunk size to 1 and read elements from a file one by one
        .read
        .fold(Vector.empty[JData])(_ :+ _.build())

    (write ++ read)
      .map(_ should be(javaData))
      .compile
      .lastOrError
  }
}
