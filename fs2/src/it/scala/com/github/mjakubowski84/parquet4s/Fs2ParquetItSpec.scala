package com.github.mjakubowski84.parquet4s

import cats.effect.testing.scalatest.AsyncIOSpec
import cats.effect.{IO, Ref}
import cats.implicits.*
import com.github.mjakubowski84.parquet4s.parquet.rotatingWriter
import fs2.Stream
import fs2.io.file.Files
import org.apache.parquet.hadoop.metadata.CompressionCodecName
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT64
import org.apache.parquet.schema.Type.*
import org.apache.parquet.schema.{MessageType, Types}
import org.scalatest.Inspectors
import org.scalatest.flatspec.AsyncFlatSpec
import org.scalatest.matchers.should.Matchers

import scala.collection.compat.immutable.LazyList
import scala.concurrent.duration.*
import scala.util.Random
import org.apache.parquet.hadoop.ParquetFileWriter

object Fs2ParquetItSpec {

  case class Data(i: Long, s: String)

  case class DataPartitioned(i: Long, s: String, a: String, b: String)

  object DataTransformed {
    def apply(data: Data, partition: String): DataTransformed = DataTransformed(data.i, data.s, partition)
  }
  case class DataTransformed(i: Long, s: String, partition: String)

  implicit def parquetPathToFs2Path(path: Path): fs2.io.file.Path = fs2.io.file.Path.fromNioPath(path.toNio)

  implicit class Fs2PathWrapper(fs2Path: fs2.io.file.Path) {
    def toPath: Path = Path(fs2Path.toNioPath)
  }

}

class Fs2ParquetItSpec extends AsyncFlatSpec with AsyncIOSpec with Matchers with Inspectors {

  import Fs2ParquetItSpec.*

  val writeOptions: ParquetWriter.Options = ParquetWriter.Options(
    compressionCodecName = CompressionCodecName.SNAPPY,
    pageSize             = 512,
    rowGroupSize         = 4 * 512
  )
  val RowGroupsPerFile: Int = 4
  val count: Long           = RowGroupsPerFile * writeOptions.rowGroupSize
  val dictS: Seq[String]    = Vector("a", "b", "c", "d")
  val dictA: Seq[String]    = Vector("1", "2", "3")
  val dictB: Seq[String]    = Vector("x", "y", "z")
  val data: LazyList[Data] = LazyList
    .range(start = 0L, end = count, step = 1L)
    .map(i => Data(i = i, s = dictS(Random.nextInt(4))))
  val dataPartitioned: LazyList[DataPartitioned] = LazyList
    .range(start = 0L, end = count, step = 1L)
    .map(i =>
      DataPartitioned(
        i = i,
        s = dictS(Random.nextInt(4)),
        a = dictA(Random.nextInt(3)),
        b = dictB(Random.nextInt(3))
      )
    )
  val vcc: ValueCodecConfiguration = ValueCodecConfiguration.Default

  def read[T: ParquetRecordDecoder](path: Path): Stream[IO, Vector[T]] =
    parquet.fromParquet[IO].as[T].read(path).fold(Vector.empty[T])(_ :+ _)

  def readSize[T: ParquetRecordDecoder](path: Path): Stream[IO, Long] =
    parquet.fromParquet[IO].as[T].read(path).fold(0L) { case (acc, _) => acc + 1L }

  def listParquetFiles(path: Path): Stream[IO, Vector[Path]] =
    Files[IO]
      .walk(path)
      .map(_.toPath)
      .filter(_.toString.endsWith(".parquet"))
      .fold(Vector.empty[Path])(_ :+ _)

  "FS2 Parquet" should "write and read single parquet file" in {
    val outputFileName = "data.parquet"
    def write(path: Path): Stream[IO, Nothing] =
      Stream
        .iterable(data)
        .through(parquet.writeSingleFile[IO].of[Data].options(writeOptions).write(path.append(outputFileName)))

    val testStream =
      for {
        path     <- Stream.resource(Files[IO].tempDirectory(None, "", None)).map(_.toPath)
        readData <- write(path) ++ read[Data](path)
      } yield readData should contain theSameElementsInOrderAs data

    testStream.compile.lastOrError
  }

  it should "write and read single parquet file using projection" in {
    val outputFileName = "data.parquet"
    def write(path: Path): Stream[IO, Nothing] =
      Stream
        .iterable(data)
        .through(parquet.writeSingleFile[IO].of[Data].options(writeOptions).write(path.append(outputFileName)))

    implicit val projectedSchema: MessageType = Types
      .buildMessage()
      .addField(
        Types.primitive(INT64, Repetition.REQUIRED).named("i")
      )
      .named("projected-schema")

    def readProjected[T: ParquetRecordDecoder: ParquetSchemaResolver](path: Path): Stream[IO, Vector[T]] =
      parquet.fromParquet[IO].projectedAs[T].read(path).fold(Vector.empty[T])(_ :+ _)

    val expectedRecords = data.map(d => RowParquetRecord.emptyWithSchema("i").updated("i", d.i, vcc))

    val testStream =
      for {
        path     <- Stream.resource(Files[IO].tempDirectory(None, "", None)).map(_.toPath)
        readData <- write(path) ++ readProjected[RowParquetRecord](path)
      } yield readData should contain theSameElementsInOrderAs expectedRecords

    testStream.compile.lastOrError
  }

  it should "write and read single parquet file using generic column projection" in {
    val outputFileName = "data.parquet"
    def write(path: Path): Stream[IO, Nothing] =
      Stream
        .iterable(data)
        .through(parquet.writeSingleFile[IO].of[Data].options(writeOptions).write(path.append(outputFileName)))

    def readProjected(path: Path): Stream[IO, Vector[RowParquetRecord]] =
      parquet
        .fromParquet[IO]
        .projectedGeneric(Col("s").as[String])
        .read(path)
        .fold(Vector.empty[RowParquetRecord])(_ :+ _)

    val testStream =
      for {
        path     <- Stream.resource(Files[IO].tempDirectory(None, "", None)).map(_.toPath)
        readData <- write(path) ++ readProjected(path)
      } yield {
        readData should have size count
        forAll(readData) { record =>
          record should have size 1
          record.get[String]("s", ValueCodecConfiguration.Default) should contain oneElementOf dictS
        }
      }

    testStream.compile.lastOrError
  }

  it should "flush already processed data to file on failure" in {
    val numberOfProcessedElementsBeforeFailure = 5
    val outputFileName                         = "data.parquet"
    def write(path: Path): Stream[IO, Nothing] =
      Stream
        .iterable(data)
        .take(numberOfProcessedElementsBeforeFailure)
        .append(Stream.raiseError[IO](new RuntimeException("test exception")))
        .through(parquet.writeSingleFile[IO].of[Data].options(writeOptions).write(path.append(outputFileName)))
        .handleErrorWith(_ => Stream.empty)

    val testStream =
      for {
        path     <- Stream.resource(Files[IO].tempDirectory(None, "", None)).map(_.toPath)
        readData <- write(path) ++ read[Data](path)
      } yield readData should contain theSameElementsInOrderAs data.take(numberOfProcessedElementsBeforeFailure)

    testStream.compile.lastOrError
  }

  it should "write files and rotate by max file size" in {
    val maxCount              = writeOptions.rowGroupSize
    val expectedNumberOfFiles = RowGroupsPerFile

    def write(path: Path): Stream[IO, Vector[Data]] =
      Stream
        .iterable(data)
        .through(
          parquet
            .viaParquet[IO]
            .of[Data]
            .maxCount(maxCount)
            .options(writeOptions)
            .write(path)
        )
        .fold(Vector.empty[Data])(_ :+ _)

    val testStream =
      for {
        path         <- Stream.resource(Files[IO].tempDirectory(None, "", None)).map(_.toPath)
        writtenData  <- write(path)
        readData     <- read[Data](path)
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
        .iterable[IO, Data](data)
        .metered(50.nanos)
        .through(
          parquet
            .viaParquet[IO]
            .of[Data]
            .maxDuration(50.millis)
            .maxCount(count)
            .options(writeOptions)
            .write(path)
        )
        .fold(Vector.empty[Data])(_ :+ _)

    val testStream =
      for {
        path         <- Stream.resource(Files[IO].tempDirectory(None, "", None)).map(_.toPath)
        writtenData  <- write(path)
        readData     <- read[Data](path)
        parquetFiles <- listParquetFiles(path)
      } yield {
        writtenData should contain theSameElementsAs data
        readData should contain theSameElementsAs data
        parquetFiles.size should be > 1
      }

    testStream.compile.lastOrError
  }

  it should "apply postWriteHandler" in {
    val expectedNumberOfFiles = 8
    val countOverride         = count / expectedNumberOfFiles

    def write(path: Path, gaugeRef: Ref[IO, Vector[Long]]): Stream[IO, Vector[Data]] =
      Stream
        .iterable(data)
        .through(
          parquet
            .viaParquet[IO]
            .of[Data]
            .maxCount(count)
            .postWriteHandler { state =>
              state.modifiedPartitions.toList.traverse_ {
                case (path, count) if count >= countOverride =>
                  gaugeRef.update(_ :+ count) >> state.flush(path)
                case _ =>
                  IO.unit
              }
            }
            .options(writeOptions)
            .write(path)
        )
        .fold(Vector.empty[Data])(_ :+ _)

    val testStream =
      for {
        path         <- Stream.resource(Files[IO].tempDirectory(None, "", None)).map(_.toPath)
        gaugeRef     <- Stream.eval(Ref.of[IO, Vector[Long]](Vector.empty))
        writtenData  <- write(path, gaugeRef)
        readData     <- read[Data](path)
        parquetFiles <- listParquetFiles(path)
        gaugeValue   <- Stream.eval(gaugeRef.get)
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
        .through(
          parquet
            .viaParquet[IO]
            .of[DataPartitioned]
            .maxCount(count)
            .partitionBy(Col("a"), Col("b"))
            .options(writeOptions)
            .write(path)
        )
        .fold(Vector.empty[DataPartitioned])(_ :+ _)

    def partitionValue(path: Path): (String, String) = {
      val split = path.name.split("=")
      (split(0), split(1))
    }

    val testStream =
      for {
        path         <- Stream.resource(Files[IO].tempDirectory(None, "", None)).map(_.toPath)
        writtenData  <- write(path)
        parquetFiles <- listParquetFiles(path)
        readData     <- read[DataPartitioned](path)
      } yield {
        writtenData should contain theSameElementsAs dataPartitioned
        parquetFiles.size should be > 1
        val partitions = parquetFiles.map { path =>
          (partitionValue(path.parent.get.parent.get), partitionValue(path.parent.get))
        }
        forEvery(partitions) { case ((a, aVal), (b, bVal)) =>
          a should be("a")
          b should be("b")
          dictA should contain(aVal)
          dictB should contain(bVal)
        }
        readData should contain theSameElementsAs dataPartitioned
      }

    testStream.compile.lastOrError
  }

  it should "transform data before writing" in {
    val partitions         = Set("x", "y", "z")
    val partitionSize: Int = (count / partitions.size).toInt
    val partitionData      = data.take(partitionSize)

    def write(path: Path): Stream[IO, Vector[Data]] =
      Stream
        .iterable(partitionData)
        .through(
          parquet
            .viaParquet[IO]
            .of[Data]
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
        .fromParquet[IO]
        .projectedAs[DataTransformed]
        .read(path)
        .map { case DataTransformed(i, s, partition) => Map(partition -> Vector(Data(i, s))) }
        .reduceSemigroup

    val testStream =
      for {
        path            <- Stream.resource(Files[IO].tempDirectory(None, "", None)).map(_.toPath)
        writtenData     <- write(path)
        partitionPaths  <- Files[IO].list(path).map(_.toPath).fold(Vector.empty[Path])(_ :+ _)
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
    val numberOfProcessedElementsBeforeFailure = rotatingWriter.DefaultChunkSize - 2

    def write(path: Path, counter: Ref[IO, Long]): Stream[IO, Vector[Data]] =
      Stream
        .iterable(data)
        .through(
          parquet
            .viaParquet[IO]
            .of[Data]
            .options(writeOptions)
            .partitionBy(Col("s"))
            .write(path)
        )
        .evalTap { _ =>
          counter.updateAndGet(_ + 1L).flatMap {
            case count if count >= numberOfProcessedElementsBeforeFailure =>
              IO.raiseError(new RuntimeException("test exception"))
            case _ =>
              IO.unit
          }
        }
        .handleErrorWith(_ => Stream.empty)
        .fold(Vector.empty[Data])(_ :+ _)

    val testStream =
      for {
        counter    <- Stream.eval(Ref.of[IO, Long](0L))
        path       <- Stream.resource(Files[IO].tempDirectory(None, "", None)).map(_.toPath)
        outputData <- write(path, counter)
        readData   <- read[Data](path)
      } yield {
        // element that failed is not in the output but is written and can be read
        outputData should contain theSameElementsAs data.take(numberOfProcessedElementsBeforeFailure - 1)
        // writer writes data in chunks so the whole chunk is written before it is passed to the output
        readData should contain theSameElementsAs data.take(rotatingWriter.DefaultChunkSize)
      }

    testStream.compile.lastOrError
  }

  it should "flush already processed files on premature completion downstream when using rotating writer" in {
    val numberOfProcessedElementsBeforeStop = rotatingWriter.DefaultChunkSize - 2

    def write(path: Path): Stream[IO, Vector[Data]] =
      Stream
        .iterable(data)
        .through(
          parquet
            .viaParquet[IO]
            .of[Data]
            .options(writeOptions)
            .partitionBy(Col("s"))
            .write(path)
        )
        .take(numberOfProcessedElementsBeforeStop)
        .handleErrorWith(_ => Stream.empty)
        .fold(Vector.empty[Data])(_ :+ _)

    val testStream =
      for {
        path        <- Stream.resource(Files[IO].tempDirectory(None, "", None)).map(_.toPath)
        writtenData <- write(path)
        readData    <- read[Data](path)
      } yield {
        writtenData should contain theSameElementsAs data.take(numberOfProcessedElementsBeforeStop)
        // writer writes data in chunks so the whole chunk is written before it is passed to the output
        readData should contain theSameElementsAs data.take(rotatingWriter.DefaultChunkSize)
      }

    testStream.compile.lastOrError
  }

  it should "write files and rotate by max file size for each partition" in {
    val maxCount                   = 512
    val expectedNumberOfPartitions = dictS.length

    case class I(i: Int)

    def write(path: Path): Stream[IO, Vector[Data]] =
      Stream
        .iterable(data)
        .through(
          parquet
            .viaParquet[IO]
            .of[Data]
            .maxCount(maxCount)
            .options(writeOptions)
            .partitionBy(Col("s"))
            .write(path)
        )
        .fold(Vector.empty[Data])(_ :+ _)

    val testStream =
      for {
        path         <- Stream.resource(Files[IO].tempDirectory(None, "", None)).map(_.toPath)
        _            <- write(path)
        parquetFiles <- listParquetFiles(path)
        count <- Stream.iterable(parquetFiles).evalMap { path =>
          read[I](path).compile.count
        }
      } yield {
        parquetFiles.length should be > expectedNumberOfPartitions
        count.toInt should ((be <= maxCount) and be >= 1)
      }

    testStream.compile.lastOrError
  }

  it should "write files and rotate by max duration for each partition" in {
    val expectedNumberOfPartitions = dictS.length

    case class I(i: Int)

    def write(path: Path): Stream[IO, Vector[Data]] =
      Stream
        .iterable[IO, Data](data)
        .metered(50.nanos)
        .through(
          parquet
            .viaParquet[IO]
            .of[Data]
            .maxCount(count)
            .maxDuration(50.millis)
            .options(writeOptions)
            .partitionBy(Col("s"))
            .write(path)
        )
        .fold(Vector.empty[Data])(_ :+ _)

    val testStream =
      for {
        path         <- Stream.resource(Files[IO].tempDirectory(None, "", None)).map(_.toPath)
        _            <- write(path)
        parquetFiles <- listParquetFiles(path)
        total        <- readSize[I](path)
      } yield {
        parquetFiles.length should be > expectedNumberOfPartitions
        total should be(count)
      }

    testStream.compile.lastOrError
  }

  it should "add new files in create mode" in {
    def write(path: Path): Stream[IO, Data] =
      Stream
        .iterable(data)
        .take(1L)
        .through(
          parquet
            .viaParquet[IO]
            .of[Data]
            .options(writeOptions.copy(writeMode = ParquetFileWriter.Mode.CREATE))
            .write(path)
        )

    val testStream =
      for {
        path         <- Stream.resource(Files[IO].tempDirectory(None, "", None)).map(_.toPath)
        _            <- write(path) >> write(path) >> write(path)
        parquetFiles <- listParquetFiles(path)
      } yield parquetFiles should have size 3

    testStream.compile.lastOrError
  }

  it should "delete existing files in overwrite mode" in {
    def write(path: Path): Stream[IO, Data] =
      Stream
        .iterable(data)
        .take(1L)
        .through(
          parquet
            .viaParquet[IO]
            .of[Data]
            .options(writeOptions.copy(writeMode = ParquetFileWriter.Mode.OVERWRITE))
            .write(path)
        )

    val testStream =
      for {
        path         <- Stream.resource(Files[IO].tempDirectory(None, "", None)).map(_.toPath)
        _            <- write(path) >> write(path) >> write(path)
        parquetFiles <- listParquetFiles(path)
      } yield parquetFiles should have size 1

    testStream.compile.lastOrError
  }

}
