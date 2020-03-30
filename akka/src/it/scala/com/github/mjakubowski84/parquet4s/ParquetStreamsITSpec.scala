package com.github.mjakubowski84.parquet4s

import java.sql.Timestamp

import akka.actor.ActorSystem
import akka.stream.scaladsl.{Sink, Source}
import org.apache.hadoop.conf.Configuration
import org.apache.parquet.hadoop.metadata.CompressionCodecName
import org.scalatest.{AsyncFlatSpec, BeforeAndAfterAll, Inspectors, Matchers}
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.immutable
import scala.concurrent.duration._
import scala.concurrent.{Await, Future}
import scala.util.Random

object ParquetStreamsITSpec {

  case class Data(i: Long, s: String)

  case class DataPartitioned(i: Long, s: String, a: String, b: String)

  object DataTransformed {
    def apply(data: Data, t: Timestamp): DataTransformed = DataTransformed(data.i, data.s , t)
  }
  case class DataTransformed(i: Long, s: String, t: Timestamp) {
    def toData: Data = Data(i, s)
  }

}


class ParquetStreamsITSpec extends AsyncFlatSpec
  with Matchers
  with TestUtils
  with IOOps
  with Inspectors
  with BeforeAndAfterAll {

  import ParquetRecordDecoder._
  import ParquetStreamsITSpec._

  override val logger: Logger = LoggerFactory.getLogger(this.getClass)
  implicit val system: ActorSystem = ActorSystem()

  val configuration: Configuration = new Configuration()
  val writeOptions: ParquetWriter.Options = ParquetWriter.Options(
    compressionCodecName = CompressionCodecName.SNAPPY,
    pageSize = 512,
    rowGroupSize = 4 * 512,
    hadoopConf = configuration
  )

  val count: Int = 4 //* writeOptions.rowGroupSize
  val dict: Seq[String] = Vector("a", "b", "c", "d")
  val data: Stream[Data] = Stream
    .range(start = 0L, end = count, step = 1L)
    .map(i => Data(i, dict(Random.nextInt(4))))

  def read[T : ParquetRecordDecoder](path: String, filter: Filter = Filter.noopFilter): Future[immutable.Seq[T]] =
    ParquetStreams.fromParquet[T](path = path, filter = filter).runWith(Sink.seq)

  override def beforeAll(): Unit = {
    super.beforeAll()
    clearTemp()
  }

  /*
  "ParquetStreams" should "write single file and read it correctly" in {
    val outputPath = s"$tempPathString/writeSingleFile"
    val outputFileName = "data.parquet"

    val write = () => Source(data).runWith(ParquetStreams.toParquetSingleFile(
      path = s"$outputPath/$outputFileName",
      options = writeOptions
    ))

    for {
      _ <- write()
      files <- filesAtPath(outputPath, configuration)
      readData <- read[Data](outputPath)
    } yield {
      files should be(List(outputFileName))
      readData should have size count
      readData should contain theSameElementsInOrderAs data
    }
  }

  it should "be able to filter files during reading" in {
    val outputPath = s"$tempPathString/writeSingleFileAndFilterIt"
    val outputFileName = "data.parquet"

    val write = () => Source(data).runWith(ParquetStreams.toParquetSingleFile(
      path = s"$outputPath/$outputFileName",
      options = writeOptions
    ))

    for {
      _ <- write()
      readData <- read[Data](outputPath, filter = Col("s") === "a")
    } yield {
      readData should not be empty
      forAll(readData) { _.s should be("a") }
    }
  }

  it should "be able to read partitioned data" in {
    val outputPath = s"$tempPathString/readDataPartitioned"
    val outputFileName = "data.parquet"

    val write = (a: String, b: String) => Source(data).runWith(ParquetStreams.toParquetSingleFile(
      path = s"$outputPath/a=$a/b=$b/$outputFileName",
      options = writeOptions
    ))

    for {
      _ <- write("A", "1")
      _ <- write("A", "2")
      _ <- write("B", "1")
      _ <- write("B", "2")
      readData <- read[DataPartitioned](outputPath)
    } yield {
      readData should have size count * 4
      readData.filter(d => d.a == "A" && d.b == "1") should have size count
      readData.filter(d => d.a == "A" && d.b == "2") should have size count
      readData.filter(d => d.a == "B" && d.b == "1") should have size count
      readData.filter(d => d.a == "B" && d.b == "2") should have size count
    }
  }

  it should "be fail informing about inconsistent partitions at given path" in {
    val outputPath = s"$tempPathString/readInconsistentPartitions"
    val outputFileName = "data.parquet"

    val write = (midPath: String) => Source(data).runWith(ParquetStreams.toParquetSingleFile(
      path = s"$outputPath/$midPath/$outputFileName",
      options = writeOptions
    ))

    for {
      _ <- write("a=A/b=1")
      _ <- write("b=2")
      _ <- write("a=B")
      error <- read[DataPartitioned](outputPath).failed
    } yield error.getCause should be(an[AssertionError])
  }

   */
  it should "filter data by partition" in {
    val outputPath = s"$tempPathString/filterDataByPartition"
    val outputFileName = "data.parquet"

    val write = (midPath: String) => Source(data).runWith(ParquetStreams.toParquetSingleFile(
      path = s"$outputPath/$midPath/$outputFileName",
      options = writeOptions
    ))

    for {
      _ <- write("a=number/b=1")
      _ <- write("a=number/b=2")
      _ <- write("a=number/b=3")
      _ <- write("a=letter/b=a")
      _ <- write("a=letter/b=b")
      _ <- write("a=letter/b=c")
      letterA <- read[DataPartitioned](outputPath, filter = Col("b") === "a")
      letterBnC <- read[DataPartitioned](outputPath, filter = Col("a") === "letter" && (Col("b") !== "a"))
      number1n3 <- read[DataPartitioned](outputPath, filter = Col("a") === "number" && (Col("b") < "2" || Col("b") > "2"))
      number2 <- read[DataPartitioned](outputPath, filter = Col("a") === "number" && (Col("b") >= "2" && Col("b") <= "2"))
    } yield {
      letterA should not be(empty)
      forAll(letterA) { elem =>
        elem.a should be("letter")
        elem.b should be("a")
      }
      letterBnC should not be(empty)
      forAll(letterBnC) { elem =>
        elem.a should be("letter")
        elem.b should (be("b") or be("c"))
      }
      number1n3 should not be(empty)
      forAll(number1n3) { elem =>
        elem.a should be("number")
        elem.b should (be("1") or be("3"))
      }
      number2 should not be(empty)
      forAll(number2) { elem =>
        elem.a should be("number")
        elem.b should be("2")
      }
    }
  }


  /*
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
      files <- filesAtPath(outputPath, configuration)
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
      files <- filesAtPath(outputPath, configuration)
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
      files <- filesAtPath(outputPath, configuration)
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
      files <- filesAtPath(outputPath, configuration)
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

  */

  override def afterAll(): Unit = {
    Await.ready(system.terminate(), Duration.Inf)
    super.afterAll()
  }

}
