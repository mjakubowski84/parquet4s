package com.github.mjakubowski84.parquet4s

import cats.effect.testing.scalatest.AsyncIOSpec

import cats.effect.{IO, Ref}
import cats.implicits._
import fs2.Stream
import fs2.io.file.Files
import org.apache.parquet.hadoop.metadata.CompressionCodecName
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT64
import org.apache.parquet.schema.Type._
import org.apache.parquet.schema.{MessageType, Types}
import org.scalatest.Inspectors
import org.scalatest.flatspec.AsyncFlatSpec
import org.scalatest.matchers.should.Matchers

import scala.collection.compat.immutable.LazyList
import scala.concurrent.duration._
import scala.util.Random

object Fs2ParquetItSpec {

  case class Data(i: Long, s: String)

  case class DataPartitioned(i: Long, s: String, a: String, b: String)

  object DataTransformed {
    def apply(data: Data, partition: String): DataTransformed = DataTransformed(data.i, data.s, partition)
  }
  case class DataTransformed(i: Long, s: String, partition: String)

}

class Fs2ParquetItSpec extends AsyncFlatSpec with AsyncIOSpec with Matchers with Inspectors {

  import Fs2ParquetItSpec._

  val writeOptions: ParquetWriter.Options = ParquetWriter.Options(
    compressionCodecName = CompressionCodecName.SNAPPY,
    pageSize = 512,
    rowGroupSize = 4 * 512
  )
  val RowGroupsPerFile: Int = 4
  val count: Int = RowGroupsPerFile * writeOptions.rowGroupSize
  val dictS: Seq[String] = Vector("a", "b", "c", "d")
  val dictA: Seq[String] = Vector("1", "2", "3")
  val dictB: Seq[String] = Vector("x", "y", "z")
  val data: LazyList[Data] = LazyList
    .range(start = 0L, end = count.toLong, step = 1L)
    .map(i => Data(i = i, s = dictS(Random.nextInt(4))))
  val dataPartitioned: LazyList[DataPartitioned] = LazyList
    .range(start = 0L, end = count.toLong, step = 1L)
    .map(i => DataPartitioned(
      i = i,
      s = dictS(Random.nextInt(4)),
      a = dictA(Random.nextInt(3)),
      b = dictB(Random.nextInt(3))
    ))
  val vcc: ValueCodecConfiguration = ValueCodecConfiguration.Default

  def read[T: ParquetRecordDecoder](path: Path): Stream[IO, Vector[T]] =
    parquet.fromParquet[IO, T].read(path).fold(Vector.empty[T])(_ :+ _)

  def listParquetFiles(path: Path): Stream[IO, Vector[Path]] =
    Files[IO].directoryStream(path.toNio)
      .map(Path.apply)
      .filter(_.toString.endsWith(".parquet"))
      .fold(Vector.empty[Path])(_ :+ _)

  it should "write and read single parquet file" in {
    val outputFileName = "data.parquet"
    def write(path: Path): Stream[IO, fs2.INothing] =
      Stream
        .iterable(data)
        .through(parquet.writeSingleFile[IO, Data](path.append(outputFileName), writeOptions))

    val testStream =
      for {
        path <- Stream.resource(Files[IO].tempDirectory()).map(Path.apply)
        readData <- write(path) ++ read[Data](path)
      } yield readData should contain theSameElementsInOrderAs data

    testStream.compile.lastOrError
  }

  it should "write and read single parquet file using projection" in {
    val outputFileName = "data.parquet"
    def write(path: Path): Stream[IO, fs2.INothing] =
      Stream
        .iterable(data)
        .through(parquet.writeSingleFile[IO, Data](path.append(outputFileName), writeOptions))

    implicit val projectedSchema: MessageType = Types.buildMessage().addField(
      Types.primitive(INT64, Repetition.REQUIRED).named("i")
    ).named("projected-schema")

    def readProjected[T: ParquetRecordDecoder: ParquetSchemaResolver](path: Path): Stream[IO, Vector[T]] =
      parquet.fromParquet[IO, T].projection.read(path).fold(Vector.empty[T])(_ :+ _)

    val expectedRecords = data.map(d => RowParquetRecord.emptyWithSchema("i").updated("i", d.i, vcc))

    val testStream =
      for {
        path <- Stream.resource(Files[IO].tempDirectory()).map(Path.apply)
        readData <- write(path) ++ readProjected[RowParquetRecord](path)
      } yield readData should contain theSameElementsInOrderAs expectedRecords

    testStream.compile.lastOrError
  }

  it should "flush already processed data to file on failure" in {
    val numberOfProcessedElementsBeforeFailure = 5
    val outputFileName = "data.parquet"
    def write(path: Path): Stream[IO, fs2.INothing] =
      Stream
        .iterable(data)
        .take(numberOfProcessedElementsBeforeFailure)
        .append(Stream.raiseError[IO](new RuntimeException("test exception")))
        .through(parquet.writeSingleFile[IO, Data](path.append(outputFileName), writeOptions))
        .handleErrorWith(_ => Stream.empty)

    val testStream =
      for {
        path <- Stream.resource(Files[IO].tempDirectory()).map(Path.apply)
        readData <- write(path) ++ read[Data](path)
      } yield readData should contain theSameElementsInOrderAs data.take(numberOfProcessedElementsBeforeFailure)

    testStream.compile.lastOrError
  }

  it should "write files and rotate by max file size" in {
    val maxCount = writeOptions.rowGroupSize
    val expectedNumberOfFiles = RowGroupsPerFile

    def write(path: Path): Stream[IO, Vector[Data]] =
      Stream
        .iterable(data)
        .through(parquet.viaParquet[IO, Data]
          .maxCount(maxCount)
          .options(writeOptions)
          .write(path)
        )
        .fold(Vector.empty[Data])(_ :+ _)

    val testStream =
      for {
        path <- Stream.resource(Files[IO].tempDirectory()).map(Path.apply)
        writtenData <- write(path)
        readData <- read[Data](path)
        parquetFiles <- listParquetFiles(path)
      } yield {
        writtenData should contain theSameElementsAs data
        readData should contain theSameElementsAs data
        parquetFiles should have size expectedNumberOfFiles
      }

    testStream.compile.lastOrError
  }

  it should "write files and rotate by max write duration" in {
    def write(path: Path): Stream[IO, Vector[Data]] =
      Stream
        .iterable(data)
        .through(parquet.viaParquet[IO, Data]
          .maxDuration(25.millis)
          .maxCount(count)
          .options(writeOptions)
          .write(path)
        )
        .fold(Vector.empty[Data])(_ :+ _)

    val testStream =
      for {
        path <- Stream.resource(Files[IO].tempDirectory()).map(Path.apply)
        writtenData <- write(path)
        readData <- read[Data](path)
        parquetFiles <- listParquetFiles(path)
      } yield {
        writtenData should contain theSameElementsAs data
        readData should contain theSameElementsAs data
        parquetFiles.size should be > 1
      }

    testStream.compile.lastOrError
  }

  it should "apply postWriteHandlerWhenWriting" in {
    val expectedNumberOfFiles = 8
    val countOverride = count / expectedNumberOfFiles

    def write(path: Path, gaugeRef: Ref[IO, Vector[Long]]): Stream[IO, Vector[Data]] =
      Stream
        .iterable(data)
        .through(parquet.viaParquet[IO, Data]
          .maxCount(count)
          .postWriteHandler {
            case state if state.count >= countOverride =>
              gaugeRef.update(_ :+ state.count) >> state.flush
            case _ => IO.unit
          }
          .options(writeOptions)
          .write(path)
        )
        .fold(Vector.empty[Data])(_ :+ _)

    val testStream =
      for {
        path <- Stream.resource(Files[IO].tempDirectory()).map(Path.apply)
        gaugeRef <- Stream.eval(Ref.of[IO, Vector[Long]](Vector.empty))
        writtenData <- write(path, gaugeRef)
        readData <- read[Data](path)
        parquetFiles <- listParquetFiles(path)
        gaugeValue <- Stream.eval(gaugeRef.get)
      } yield {
        writtenData should contain theSameElementsAs data
        readData should contain theSameElementsAs data
        parquetFiles should have size expectedNumberOfFiles
        gaugeValue should be(Vector.fill(expectedNumberOfFiles)(countOverride))
      }

    testStream.compile.lastOrError
  }

  it should "write and read partitioned files" in {
    def write(path: Path): Stream[IO, Vector[DataPartitioned]] =
      Stream
        .iterable(dataPartitioned)
        .through(parquet.viaParquet[IO, DataPartitioned]
          .maxCount(count)
          .partitionBy(Col("a"), Col("b"))
          .options(writeOptions)
          .write(path)
        )
        .fold(Vector.empty[DataPartitioned])(_ :+ _)

    def listParquetFiles(path: Path): Stream[IO, Vector[Path]] =
      Files[IO].walk(path.toNio)
        .map(Path.apply)
        .filter(_.name.endsWith(".parquet"))
        .fold(Vector.empty[Path])(_ :+ _)

    def partitionValue(path: Path): (String, String) = {
      val split = path.name.split("=")
      (split(0), split(1))
    }

    val testStream =
      for {
        path <- Stream.resource(Files[IO].tempDirectory()).map(Path.apply)
        writtenData <- write(path)
        parquetFiles <- listParquetFiles(path)
        readData <- read[DataPartitioned](path)
      } yield {
        writtenData should contain theSameElementsAs dataPartitioned
        parquetFiles.size should be > 1
        val partitions = parquetFiles.map { path =>
          (partitionValue(path.parent.get.parent.get), partitionValue(path.parent.get))
        }
        forEvery(partitions) { case (("a", aVal), ("b", bVal)) =>
          dictA should contain(aVal)
          dictB should contain(bVal)
        }
        readData should contain theSameElementsAs dataPartitioned
      }

    testStream.compile.lastOrError
  }

  it should "transform data before writing" in {
    val partitions = Set("x", "y", "z")
    val partitionSize = count / partitions.size
    val partitionData = data.take(partitionSize)

    def write(path: Path): Stream[IO, Vector[Data]] =
      Stream
        .iterable(partitionData)
        .through(parquet.viaParquet[IO, Data]
          .maxCount(partitionSize)
          .preWriteTransformation[DataTransformed] { data =>
            Stream.iterable(partitions).map(partition => DataTransformed(data, partition))
          }
          .partitionBy(Col("partition"))
          .options(writeOptions)
          .write(path)
        )
        .fold(Vector.empty[Data])(_ :+ _)

    def read(path: Path): Stream[IO, Map[String, Vector[Data]]] =
      parquet
        .fromParquet[IO, DataTransformed].read(path)
        .map { case DataTransformed(i, s, partition) => Map(partition -> Vector(Data(i, s))) }
        .reduceSemigroup

    val testStream =
      for {
        path <- Stream.resource(Files[IO].tempDirectory()).map(Path.apply)
        writtenData <- write(path)
        partitionPaths <- Files[IO].directoryStream(path.toNio).map(Path.apply).fold(Vector.empty[Path])(_ :+ _)
        partitionedData <- read(path)
      } yield {
        writtenData should contain theSameElementsAs partitionData
        partitionPaths should have size partitions.size
        forEvery(partitionPaths)(_.name should fullyMatch regex "partition=[xyz]")
        partitionedData.keys should be(partitions)
        forEvery(partitionedData.keys) { partition =>
          partitionedData(partition) should contain theSameElementsAs partitionData
        }
      }

    testStream.compile.lastOrError
  }

  it should "flush already processed files on failure when using rotating writer" in {
    val numberOfProcessedElementsBeforeFailure = 25

    def write(path: Path): Stream[IO, Vector[Data]] =
      Stream
        .iterable(data)
        .through(parquet.viaParquet[IO, Data]
          .options(writeOptions)
          .partitionBy(Col("s"))
          .postWriteHandler {
            case state if state.count >= numberOfProcessedElementsBeforeFailure =>
              IO.raiseError(new RuntimeException("test exception"))
            case _ =>
              IO.unit
          }
          .write(path)
        )
        .handleErrorWith(_ => Stream.empty)
        .fold(Vector.empty[Data])(_ :+ _)

    val testStream =
      for {
        path <- Stream.resource(Files[IO].tempDirectory()).map(Path.apply)
        writtenData <- write(path)
        readData <- read[Data](path)
      } yield {
        writtenData should contain theSameElementsAs data.take(numberOfProcessedElementsBeforeFailure)
        readData should contain theSameElementsAs data.take(numberOfProcessedElementsBeforeFailure)
      }

    testStream.compile.lastOrError
  }

  it should "flush already processed files on premature completion downstream when using rotating writer" in {
    val numberOfProcessedElementsBeforeStop = 25

    def write(path: Path): Stream[IO, Vector[Data]] =
      Stream
        .iterable(data)
        .through(parquet.viaParquet[IO, Data]
          .options(writeOptions)
          .partitionBy(Col("s"))
          .write(path)
        )
        .take(numberOfProcessedElementsBeforeStop)
        .handleErrorWith(_ => Stream.empty)
        .fold(Vector.empty[Data])(_ :+ _)

    val testStream =
      for {
        path <- Stream.resource(Files[IO].tempDirectory()).map(Path.apply)
        writtenData <- write(path)
        readData <- read[Data](path)
      } yield {
        writtenData should contain theSameElementsAs data.take(numberOfProcessedElementsBeforeStop)
        readData should contain theSameElementsAs data.take(numberOfProcessedElementsBeforeStop)
      }

    testStream.compile.lastOrError
  }

}
