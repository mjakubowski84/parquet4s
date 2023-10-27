package com.github.mjakubowski84.parquet4s

import com.github.mjakubowski84.parquet4s.ScalaCompat.actor.ActorSystem
import com.github.mjakubowski84.parquet4s.ScalaCompat.stream.scaladsl.{Keep, Sink, Source}
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

class Parquet4sScalaPBAkkaPekkoSpec extends AsyncFlatSpec with Matchers with BeforeAndAfterAll {

  implicit private val system: ActorSystem = ActorSystem()

  override def afterAll(): Unit = Await.ready(system.terminate(), Duration.Inf)

  "akka-pekko module" should "be compatible with parquet-protobuf" in {
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
          .read(_.build())
      )
      .toMat(Sink.seq)(Keep.right)
    // Due to bug in ProtoParquetReader - the fact that read elements are unsafe instances of JData.Builder - and
    // as Akka Streams always try to read more than a single element from the stream (even when you change the buffer
    // size), we have to call `build()` function as soon as it is possible. Otherwise the data from the next element
    // will override properties of the previous one.

    for {
      _        <- Source.fromIterator(() => scalaData.iterator).runWith(writeSink)
      readData <- readStream.run()
    } yield readData should be(javaData)
  }
}
