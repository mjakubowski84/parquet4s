package com.github.mjakubowski84.parquet4s

import akka.actor.ActorSystem
import akka.stream.Attributes
import akka.stream.scaladsl.{Keep, Sink, Source}
import com.github.mjakubowski84.parquet4s.DataOuterClass.Data as JData
import com.github.mjakubowski84.parquet4s.ScalaPBImplicits.*
import com.github.mjakubowski84.parquet4s.TestData.*
import org.apache.hadoop.conf.Configuration
import org.apache.parquet.proto.{ProtoParquetReader, ProtoParquetWriter, ProtoReadSupport, ProtoWriteSupport}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AsyncFlatSpec
import org.scalatest.matchers.should.Matchers

import scala.concurrent.Await
import scala.concurrent.duration.Duration

class Parquet4sScalaPBAkkaSpec extends AsyncFlatSpec with Matchers with BeforeAndAfterAll {

  implicit private val system: ActorSystem = ActorSystem()

  override def afterAll(): Unit = Await.ready(system.terminate(), Duration.Inf)

  "akka module" should "be compatible with parquet-protobuf" in {
    val outFile    = InMemoryOutputFile(initBufferSize = 4800)
    val hadoopConf = new Configuration()
    hadoopConf.setBoolean(ProtoWriteSupport.PB_SPECS_COMPLIANT_WRITE, true)

    val writeSink = ParquetStreams.toParquetSingleFile
      .custom[JData, ProtoParquetWriter.Builder[JData]](
        ProtoParquetWriter.builder[JData](outFile).withMessage(classOf[JData])
      )
      .options(ParquetWriter.Options(hadoopConf = hadoopConf))
      .write

    val readSource = Source.lazySource(() => ParquetStreams.fromParquet.as[Data].read(outFile.toInputFile))

    for {
      _        <- Source.fromIterator(() => javaData.iterator).runWith(writeSink)
      readData <- readSource.runWith(Sink.seq)
    } yield readData should be(scalaData)
  }

  it should "write data compliant with parquet-protobuf" in {
    val outFile    = InMemoryOutputFile(initBufferSize = 4800)
    val hadoopConf = new Configuration()
    hadoopConf.setClass(ProtoReadSupport.PB_CLASS, classOf[JData], classOf[com.google.protobuf.GeneratedMessageV3])

    val writeSink = ParquetStreams.toParquetSingleFile.of[Data].write(outFile)

    val readStream = Source
      .lazySource(() =>
        ParquetStreams.fromParquet
          .custom[JData.Builder](ProtoParquetReader.builder[JData.Builder](outFile.toInputFile))
          .options(ParquetReader.Options(hadoopConf = hadoopConf))
          .read
          .map(_.build())
      )
      .toMat(Sink.seq)(Keep.right)
      .withAttributes(Attributes.inputBuffer(initial = 1, max = 1))
    // due to bug in ProtoParquetReader - the fact that read elements are unsafe instances of JData.Builder
    // we must limit stream buffer to 1 and read elements from a file one by one

    for {
      _        <- Source.fromIterator(() => scalaData.iterator).runWith(writeSink)
      readData <- readStream.run()
    } yield readData should be(javaData)
  }
}
