package com.github.mjakubowski84.parquet4s

import akka.actor.ActorSystem
import akka.stream.scaladsl.{Sink, Source}
import akka.stream.{ActorMaterializer, Materializer}
import org.apache.parquet.hadoop.metadata.CompressionCodecName
import org.scalatest.{AsyncFlatSpec, Matchers}
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.immutable
import scala.concurrent.{Await, Future}
import scala.concurrent.duration.Duration
import scala.util.Random

object ParquetStreamsITSpec {
  case class Data(i: Long, s: String)
}

class ParquetStreamsITSpec extends AsyncFlatSpec
  with Matchers
  with SparkHelper
  with IOOps {

  import ParquetRecordDecoder._
  import ParquetStreamsITSpec._

  override val logger: Logger = LoggerFactory.getLogger(this.getClass)
  implicit val system: ActorSystem = ActorSystem()
  implicit val mat: Materializer = ActorMaterializer()

  val writeOptions: ParquetWriter.Options = ParquetWriter.Options().copy(
    compressionCodecName = CompressionCodecName.SNAPPY,
    pageSize = 512,
    rowGroupSize = 4 * 512
  )

  val count: Int = 4 * writeOptions.rowGroupSize
  val dict: Seq[String] = Vector("a", "b", "c", "d")
  val data: Stream[Data] = Stream
    .continually(Data(Random.nextLong(), dict(Random.nextInt(4))))
    .take(count)

  def read(path: String): Future[immutable.Seq[Data]] = ParquetStreams.fromParquet[Data](path).runWith(Sink.seq)

  override def beforeAll(): Unit = {
    super.beforeAll()
    clearTemp()
  }

  "ParquetStreams" should "write single file and read it correctly" in {
    val outputPath = s"$tempPathString/writeSingleFile"
    val outputFileName = "data.parquet"

    val write = () => Source(data).runWith(ParquetStreams.toParquetSingleFile(
      path = s"$outputPath/$outputFileName",
      options = writeOptions
    ))

    for {
      _ <- write()
      files <- filesAtPath(outputPath)
      readData <- read(outputPath)
    } yield {
      files should be(List(outputFileName))
      readData should have size count
      readData should contain theSameElementsInOrderAs data
    }
  }

  it should "split data into sequential chunks and read it correctly" in {
    val outputPath = s"$tempPathString/writeSequentiallySplitFiles"
    val outputFileNames = List("part-00000.parquet", "part-00001.parquet")

    val write = () => Source(data).runWith(ParquetStreams.toParquetSequentialWithFileSplit(
      path = outputPath,
      maxRecordsPerFile = 2 * writeOptions.rowGroupSize,
      options = writeOptions
    ))

    for {
      _ <- write()
      files <- filesAtPath(outputPath)
      readData <- read(outputPath)
    } yield {
      files should contain theSameElementsAs outputFileNames
      readData should have size count
      readData should contain theSameElementsAs data // TODO check how files can be always read in correct order
    }
  }

  it should "write data in parallel as chunks and read it correctly" in {
    val outputPath = s"$tempPathString/writeParallelUnordered"

    val write = () => Source(data).runWith(ParquetStreams.toParquetParallelUnordered(
      path = outputPath,
      parallelism = 2,
      options = writeOptions
    ))

    for {
      _ <- write()
      files <- filesAtPath(outputPath)
      readData <- read(outputPath)
    } yield {
      files should have size 2
      readData should have size count
      readData should contain theSameElementsAs data
    }
  }

  override def afterAll(): Unit = {
    Await.ready(system.terminate(), Duration.Inf)
    super.afterAll()
  }

}
