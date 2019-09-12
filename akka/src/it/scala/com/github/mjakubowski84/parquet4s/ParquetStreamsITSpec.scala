package com.github.mjakubowski84.parquet4s

import java.sql.Timestamp

import akka.actor.ActorSystem
import akka.stream.scaladsl.{Sink, Source}
import akka.stream.{ActorMaterializer, Materializer}
import org.apache.parquet.hadoop.metadata.CompressionCodecName
import org.scalatest.{AsyncFlatSpec, Inspectors, Matchers}
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.immutable
import scala.concurrent.duration._
import scala.concurrent.{Await, Future}
import scala.util.Random

object ParquetStreamsITSpec {

  case class Data(i: Long, s: String)

  object DataTransformed {
    def apply(data: Data, t: Timestamp): DataTransformed = DataTransformed(data.i, data.s , t)
  }
  case class DataTransformed(i: Long, s: String, t: Timestamp) {
    def toData: Data = Data(i, s)
  }

}

class ParquetStreamsITSpec extends AsyncFlatSpec
  with Matchers
  with SparkHelper
  with IOOps
  with Inspectors {

  import ParquetRecordDecoder._
  import ParquetStreamsITSpec._

  override val logger: Logger = LoggerFactory.getLogger(this.getClass)
  implicit val system: ActorSystem = ActorSystem()
  implicit val mat: Materializer = ActorMaterializer()

  val writeOptions: ParquetWriter.Options = ParquetWriter.Options(
    compressionCodecName = CompressionCodecName.SNAPPY,
    pageSize = 512,
    rowGroupSize = 4 * 512
  )

  val count: Int = 4 * writeOptions.rowGroupSize
  val dict: Seq[String] = Vector("a", "b", "c", "d")
  val data: Stream[Data] = Stream
    .range(start = 0L, end = count, step = 1L)
    .map(i => Data(i, dict(Random.nextInt(4))))

  def read[T : ParquetRecordDecoder](path: String): Future[immutable.Seq[T]] = ParquetStreams.fromParquet[T](path).runWith(Sink.seq)

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
      files <- filesAtPath(outputPath, writeOptions)
      readData <- read[Data](outputPath)
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
      files <- filesAtPath(outputPath, writeOptions)
      readData <- read[Data](outputPath)
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
      files <- filesAtPath(outputPath, writeOptions)
      readData <- read[Data](outputPath)
    } yield {
      files should have size 2
      readData should have size count
      readData should contain theSameElementsAs data
    }
  }

  it should "split data into sequential chunks using indefinite stream support with default settings" in {
    val outputPath = s"$tempPathString/writeIndefiniteDefault"

    val write = () => Source(data).runWith(ParquetStreams.toParquetIndefinite(
      path = outputPath,
      maxChunkSize = writeOptions.rowGroupSize,
      chunkWriteTimeWindow = 10.seconds,
      options = writeOptions
    ))

    for {
      _ <- write()
      files <- filesAtPath(outputPath, writeOptions)
      readData <- read[Data](outputPath)
    } yield {
      files should have size 4
      readData should have size count
      readData should contain theSameElementsAs data
    }
  }

  it should "split data into sequential chunks using indefinite stream support with custom settings" in {
    val outputPath = s"$tempPathString/writeIndefiniteCustom"
    val expectedFileNames = List("0.parquet", "2048.parquet", "4096.parquet", "6144.parquet")
    val currentTime = new Timestamp(System.currentTimeMillis())

    val write = () => Source(data).runWith(ParquetStreams.toParquetIndefinite(
      path = outputPath,
      maxChunkSize = writeOptions.rowGroupSize,
      chunkWriteTimeWindow = 10.seconds,
      buildChunkPath = { case (basePath, chunk) => basePath.suffix(s"/${chunk.head.i}.parquet")},
      preWriteTransformation = { data => DataTransformed(data, currentTime)},
      postWriteSink = Sink.fold[Seq[Data], Seq[Data]](Seq.empty[Data])(_ ++ _),
      options = writeOptions
    ))

    for {
      postWriteData <- write()
      files <- filesAtPath(outputPath, writeOptions)
      readData <- read[DataTransformed](outputPath)
    } yield {
      postWriteData should have size count
      postWriteData should contain theSameElementsAs data
      files should contain theSameElementsAs expectedFileNames
      readData should have size count
      forAll(readData) { _.t should be(currentTime) }
      readData.map(_.toData) should contain theSameElementsAs data
    }
  }

  override def afterAll(): Unit = {
    Await.ready(system.terminate(), Duration.Inf)
    super.afterAll()
  }

}
