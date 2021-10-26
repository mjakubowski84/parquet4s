package com.github.mjakubowski84.parquet4s

import java.sql.Timestamp

import akka.Done
import akka.actor.ActorSystem
import akka.stream.scaladsl.{Sink, Source}
import com.github.mjakubowski84.parquet4s.Cursor.DotPath
import org.apache.hadoop.fs.Path
import org.apache.parquet.hadoop.metadata.CompressionCodecName
import org.apache.parquet.schema.MessageType
import org.scalatest.flatspec.AsyncFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.{BeforeAndAfter, BeforeAndAfterAll, Inspectors}
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.compat.immutable.LazyList
import scala.collection.immutable
import scala.concurrent.duration._
import scala.concurrent.{Await, Future}
import scala.util.Random

object ParquetStreamsITSpec {

  case class Data(i: Long, s: String)

  case class DataPartitioned(i: Long, s: String, a: String, b: String)

  object DataTransformed {
    def apply(data: Data, t: Timestamp): DataTransformed = DataTransformed(data.i, data.s, t)
  }
  case class DataTransformed(i: Long, s: String, t: Timestamp) {
    def toData: Data = Data(i, s)
  }

}

class ParquetStreamsITSpec
    extends AsyncFlatSpec
    with Matchers
    with TestUtils
    with IOOps
    with Inspectors
    with BeforeAndAfter
    with BeforeAndAfterAll {

  import ParquetRecordDecoder._
  import ParquetStreamsITSpec._

  override val logger: Logger      = LoggerFactory.getLogger(this.getClass)
  implicit val system: ActorSystem = ActorSystem()

  val writeOptions: ParquetWriter.Options = ParquetWriter.Options(
    compressionCodecName = CompressionCodecName.SNAPPY,
    pageSize             = 512,
    rowGroupSize         = 4 * 512,
    hadoopConf           = configuration
  )

  val count: Int        = 4 * writeOptions.rowGroupSize
  val dict: Seq[String] = Vector("a", "b", "c", "d")
  val data: LazyList[Data] = LazyList
    .range(start = 0L, end = count, step = 1L)
    .map(i => Data(i = i, s = dict(Random.nextInt(4))))

  def read[T: ParquetRecordDecoder](path: Path, filter: Filter = Filter.noopFilter): Future[immutable.Seq[T]] =
    ParquetStreams.fromParquet[T].withFilter(filter).read(path.toString).runWith(Sink.seq)

  before {
    clearTemp()
  }

  "ParquetStreams" should "write single file and read it correctly" in {
    val outputFileName = "data.parquet"

    val write = () =>
      Source(data).runWith(
        ParquetStreams.toParquetSingleFile(
          path    = s"$tempPath/$outputFileName",
          options = writeOptions
        )
      )

    for {
      _        <- write()
      files    <- filesAtPath(tempPath, configuration)
      readData <- read[Data](tempPath)
    } yield {
      files should be(List(outputFileName))
      readData should have size count
      readData should contain theSameElementsInOrderAs data
    }
  }

  it should "should return empty stream when reading empty directory" in {
    fileSystem.mkdirs(tempPath)

    for {
      files    <- filesAtPath(tempPath, configuration)
      readData <- read[Data](tempPath)
    } yield {
      files should be(empty)
      readData should be(empty)
    }
  }

  it should "be able to filter files during reading" in {
    val outputFileName = "data.parquet"

    val write = () =>
      Source(data).runWith(
        ParquetStreams.toParquetSingleFile(
          path    = s"$tempPath/$outputFileName",
          options = writeOptions
        )
      )

    for {
      _        <- write()
      readData <- read[Data](tempPath, filter = Col("s") === "a")
    } yield {
      readData should not be empty
      forAll(readData)(_.s should be("a"))
    }
  }

  it should "be able to read partitioned data" in {
    val outputFileName = "data.parquet"

    val write = (a: String, b: String) =>
      Source(data).runWith(
        ParquetStreams.toParquetSingleFile(
          path    = s"$tempPath/a=$a/b=$b/$outputFileName",
          options = writeOptions
        )
      )

    for {
      _        <- write("A", "1")
      _        <- write("A", "2")
      _        <- write("B", "1")
      _        <- write("B", "2")
      readData <- read[DataPartitioned](tempPath)
    } yield {
      readData should have size count * 4
      readData.filter(d => d.a == "A" && d.b == "1") should have size count
      readData.filter(d => d.a == "A" && d.b == "2") should have size count
      readData.filter(d => d.a == "B" && d.b == "1") should have size count
      readData.filter(d => d.a == "B" && d.b == "2") should have size count
    }
  }

  it should "filter data by partition and file content" in {
    val outputFileName = "data.parquet"

    val write = (midPath: String) =>
      Source(data).runWith(
        ParquetStreams.toParquetSingleFile(
          path    = s"$tempPath/$midPath/$outputFileName",
          options = writeOptions
        )
      )

    for {
      _        <- write("a=A/b=1")
      _        <- write("a=A/b=2")
      readData <- read[DataPartitioned](tempPath, filter = Col("b") === "1" && Col("s") === "a")
    } yield {
      readData should not be empty
      forAll(readData) { elem =>
        elem.b should be("1")
        elem.s should be("a")
      }
    }
  }

  it should "fail to read data that is improperly partitioned" in {
    val outputFileName = "data.parquet"

    val write = (midPath: String) =>
      Source(data).runWith(
        ParquetStreams.toParquetSingleFile(
          path    = s"$tempPath/$midPath/$outputFileName",
          options = writeOptions
        )
      )

    val fut = for {
      _ <- write("a=A/b=1")
      _ <- write("a=A")
      _ <- read[DataPartitioned](tempPath)
    } yield ()

    recoverToSucceededIf[IllegalArgumentException](fut)
  }

  it should "use projection when reading files" in {
    case class S(s: String)

    val outputFileName = "data.parquet"

    val write = () =>
      Source(data).runWith(
        ParquetStreams.toParquetSingleFile(
          path    = s"$tempPath/$outputFileName",
          options = writeOptions
        )
      )

    for {
      _        <- write()
      files    <- filesAtPath(tempPath, configuration)
      readData <- ParquetStreams.fromParquet[S].withProjection.read(tempPathString).runWith(Sink.seq)
    } yield {
      files should be(List(outputFileName))
      readData should have size count
      readData should contain theSameElementsInOrderAs data.map(d => S(d.s))
    }
  }

  it should "split data into sequential chunks and read it correctly" in {
    val outputFileNames = List("part-00000.parquet", "part-00001.parquet")

    val write = () =>
      Source(data).runWith(
        ParquetStreams.toParquetSequentialWithFileSplit(
          path              = tempPathString,
          maxRecordsPerFile = 2 * writeOptions.rowGroupSize,
          options           = writeOptions
        )
      )

    for {
      _        <- write()
      files    <- filesAtPath(tempPath, configuration)
      readData <- read[Data](tempPath)
    } yield {
      files should contain theSameElementsAs outputFileNames
      readData should have size count
      readData should contain theSameElementsAs data
    }
  }

  it should "write data in parallel as chunks and read it correctly" in {
    val write = () =>
      Source(data).runWith(
        ParquetStreams.toParquetParallelUnordered(
          path        = tempPathString,
          parallelism = 2,
          options     = writeOptions
        )
      )

    for {
      _        <- write()
      files    <- filesAtPath(tempPath, configuration)
      readData <- read[Data](tempPath)
    } yield {
      files should have size 2
      readData should have size count
      readData should contain theSameElementsAs data
    }
  }

  it should "split data into sequential chunks using indefinite stream support with default settings" in {
    val write = () =>
      Source(data).runWith(
        ParquetStreams.toParquetIndefinite(
          path                 = tempPathString,
          maxChunkSize         = writeOptions.rowGroupSize,
          chunkWriteTimeWindow = 10.seconds,
          options              = writeOptions
        )
      )

    for {
      _        <- write()
      files    <- filesAtPath(tempPath, configuration)
      readData <- read[Data](tempPath)
    } yield {
      files should have size 4
      readData should have size count
      readData should contain theSameElementsAs data
    }
  }

  it should "split data into sequential chunks using indefinite stream support with custom settings" in {
    val expectedFileNames = List("0.parquet", "2048.parquet", "4096.parquet", "6144.parquet")
    val currentTime       = new Timestamp(System.currentTimeMillis())

    val write = () =>
      Source(data).runWith(
        ParquetStreams.toParquetIndefinite(
          path                   = tempPathString,
          maxChunkSize           = writeOptions.rowGroupSize,
          chunkWriteTimeWindow   = 10.seconds,
          buildChunkPath         = { case (basePath, chunk) => basePath.suffix(s"/${chunk.head.i}.parquet") },
          preWriteTransformation = { data => DataTransformed(data, currentTime) },
          postWriteSink          = Sink.fold[Seq[Data], Seq[Data]](Seq.empty[Data])(_ ++ _),
          options                = writeOptions
        )
      )

    for {
      postWriteData <- write()
      files         <- filesAtPath(tempPath, configuration)
      readData      <- read[DataTransformed](tempPath)
    } yield {
      postWriteData should have size count
      postWriteData should contain theSameElementsAs data
      files should contain theSameElementsAs expectedFileNames
      readData should have size count
      forAll(readData)(_.t should be(currentTime))
      readData.map(_.toData) should contain theSameElementsAs data
    }
  }

  it should "write and read partitioned data" in {

    case class User(name: String, address: Address)
    case class Address(street: Street, country: String, postCode: String)
    case class Street(name: String, more: String)

    val flow = ParquetStreams
      .viaParquet[User](tempPathString)
      .withWriteOptions(writeOptions)
      .withMaxCount(writeOptions.rowGroupSize)
      .withMaxDuration(100.millis)
      .withPartitionBy("address.country", "address.postCode")
      .build()

    val users = Seq(
      User(name = "John", address = Address(street = Street("Broad St", "12"), country = "ABC", postCode = "123456"))
    ).toList

    val firstPartitionPath  = tempPath.suffix("/address.country=ABC")
    val secondPartitionPath = firstPartitionPath.suffix("/address.postCode=123456")

    for {
      writtenData <- Source(users).via(flow).runWith(Sink.seq)
      readData    <- ParquetStreams.fromParquet[User].read(tempPathString).runWith(Sink.seq)
      rootFiles            = fileSystem.listStatus(tempPath).map(_.getPath).toSeq
      firstPartitionFiles  = fileSystem.listStatus(firstPartitionPath).map(_.getPath).toSeq
      secondPartitionFiles = fileSystem.listStatus(secondPartitionPath).map(_.getPath.getName).toSeq
    } yield {
      rootFiles should be(Seq(firstPartitionPath))
      firstPartitionFiles should be(Seq(secondPartitionPath))
      every(secondPartitionFiles) should endWith(".snappy.parquet")
      writtenData should be(users)
      readData should be(users)
    }
  }

  it should "monitor written rows and flush on signal" in {
    case class User(name: String, id: Int, id_part: String)

    val maxCount            = 10
    val usersToWrite        = 33
    val partitions          = 2
    val lastToFlushOnDemand = 2

    def genUser(id: Int) = User(s"name_$id", id, (id % partitions).toString) // Two partitions : even and odd `id`

    val flow = ParquetStreams
      .viaParquet[User](tempPathString)
      .withWriteOptions(writeOptions)
      .withMaxCount(maxCount)
      .withPartitionBy("id_part")
      .withPostWriteHandler { state =>
        if (
          state.lastProcessed.id >= usersToWrite - lastToFlushOnDemand
        ) // use-case 2 : on demand flush. e.g: the last two records must be in separate files
          state.flush()
      }
      .build()

    val users = ((1 to usersToWrite) map genUser).toList

    for {
      writtenData <- Source(users).via(flow).runWith(Sink.seq)
      readData    <- ParquetStreams.fromParquet[User].read(tempPathString).runWith(Sink.seq)
      rootFiles = fileSystem.listStatus(tempPath).map(_.getPath).toSeq
      files     = rootFiles.flatMap(p => fileSystem.listStatus(p).map(_.getPath).toSeq)
      readDataPartitioned <- Future.sequence( // generate the file lists in a partitioned form
        files.map(file => ParquetStreams.fromParquet[User].read(file.toString).runWith(Sink.seq))
      )
    } yield {
      every(files.map(_.getName)) should endWith(".snappy.parquet")
      writtenData should be(users)

      readData should have size users.size
      readData should contain allElementsOf users

      val usersPerPartition = usersToWrite / partitions
      val filesPerPartition = (usersPerPartition / maxCount) + 1 // + 1 because the last file is partially filled
      files.length should be(filesPerPartition * partitions + lastToFlushOnDemand)

      val (full, partial) = readDataPartitioned.partition(_.length == maxCount)

      val expectedFullFiles        = (usersPerPartition / maxCount) * partitions
      val expectedUsersInFullFiles = maxCount * expectedFullFiles
      val actualUsersInFullFiles   = full.map(_.length).sum

      val expectedPartialFiles = partitions + lastToFlushOnDemand // 2 partial files + 2 prematurely flushed files
      val expectedUsersInPartialFiles = usersToWrite - expectedUsersInFullFiles
      val actualUsersInPartialFiles   = partial.map(_.length).sum

      full.length should be(expectedFullFiles)
      actualUsersInFullFiles should be(expectedUsersInFullFiles)
      partial.length should be(expectedPartialFiles)
      actualUsersInPartialFiles should be(expectedUsersInPartialFiles)

      partial.count(_.length == 1) should be(lastToFlushOnDemand) // prematurely flushed files
    }
  }

  it should "write and read data partitioned by all fields of case class" in {

    case class User(name: String, address: Address)
    case class Address(postcode: String, country: String)

    val flow = ParquetStreams
      .viaParquet[User](tempPathString)
      .withWriteOptions(writeOptions)
      .withMaxCount(writeOptions.rowGroupSize)
      .withMaxDuration(100.millis)
      .withPartitionBy("address.country", "address.postcode")
      .build()

    val users = Seq(
      User(name = "John", address = Address("123456", "ABC"))
    ).toList

    for {
      writtenData <- Source(users).via(flow).runWith(Sink.seq)
      readData    <- ParquetStreams.fromParquet[User].read(tempPathString).runWith(Sink.seq)
    } yield {
      writtenData should be(users)
      readData should be(users)
    }
  }

  it should "fail to write a case class when partitioning consumes all its fields" in {

    case class User(name: String)

    val flow = ParquetStreams
      .viaParquet[User](tempPathString)
      .withWriteOptions(writeOptions)
      .withMaxCount(writeOptions.rowGroupSize)
      .withMaxDuration(100.millis)
      .withPartitionBy("name")
      .build()

    val users = Seq(User(name = "John")).toList

    val fut = Source(users).via(flow).runWith(Sink.ignore)
    recoverToSucceededIf[org.apache.parquet.schema.InvalidSchemaException](fut)
  }

  it should "write and read data using partitioning flow but with no partition defined" in {

    case class User(name: String, address: Address)
    case class Address(postcode: String, country: String)

    val flow = ParquetStreams
      .viaParquet[User](tempPathString)
      .withWriteOptions(writeOptions)
      .withMaxCount(writeOptions.rowGroupSize)
      .withMaxDuration(100.millis)
      .build()

    val users = Seq(
      User(name = "John", address    = Address("123456", "ABC")),
      User(name = "Ben", address     = Address("111111", "CDE")),
      User(name = "Cynthia", address = Address("7654321", "XYZ"))
    ).toList

    for {
      writtenData <- Source(users).via(flow).runWith(Sink.seq)
      readData    <- ParquetStreams.fromParquet[User].read(tempPathString).runWith(Sink.seq)
    } yield {
      writtenData should be(users)
      readData should be(users)
    }
  }

  it should "write and read partitioned data using generic records" in {
    import com.github.mjakubowski84.parquet4s.ValueImplicits._

    case class User(name: String, address: Address)
    case class Address(street: Street, country: String, postCode: String)
    case class Street(name: String, more: String)

    implicit val message: MessageType = ParquetSchemaResolver.resolveSchema[User]

    val flow = ParquetStreams
      .viaParquet[RowParquetRecord](tempPathString)
      .withWriteOptions(writeOptions)
      .withMaxCount(writeOptions.rowGroupSize)
      .withMaxDuration(100.millis)
      .withPartitionBy("address.country", "address.postCode")
      .build()

    val genericUsers = Seq(
      RowParquetRecord.empty
        .add("name", "John")
        .add(DotPath("address.street.name"), "Broad St")
        .add(DotPath("address.street.more"), "12")
        .add(DotPath("address.country"), "ABC")
        .add(DotPath("address.postCode"), "123456")
    ).toList

    val expectedUsers = Seq(
      User(name = "John", address = Address(street = Street("Broad St", "12"), country = "ABC", postCode = "123456"))
    )

    val firstPartitionPath  = tempPath.suffix("/address.country=ABC")
    val secondPartitionPath = firstPartitionPath.suffix("/address.postCode=123456")

    for {
      writtenData <- Source(genericUsers).via(flow).runWith(Sink.seq)
      readData    <- ParquetStreams.fromParquet[RowParquetRecord].read(tempPathString).runWith(Sink.seq)
      rootFiles            = fileSystem.listStatus(tempPath).map(_.getPath).toSeq
      firstPartitionFiles  = fileSystem.listStatus(firstPartitionPath).map(_.getPath).toSeq
      secondPartitionFiles = fileSystem.listStatus(secondPartitionPath).map(_.getPath.getName).toSeq
    } yield {
      rootFiles should be(Seq(firstPartitionPath))
      firstPartitionFiles should be(Seq(secondPartitionPath))
      every(secondPartitionFiles) should endWith(".snappy.parquet")
      writtenData should be(genericUsers)
      readData.map(ParquetRecordDecoder.decode[User](_)) should be(expectedUsers)
    }
  }

  it should "close viaParquet properly on upstream exception" in {
    val numberOfSuccessfulWrites = 25

    val failingSource = Source(data).map {
      case data @ Data(i, _) if i < numberOfSuccessfulWrites => data
      case _                                                 => throw new RuntimeException("test exception")
    }

    val flow = ParquetStreams
      .viaParquet[Data](tempPathString)
      .withWriteOptions(writeOptions)
      .build()

    for {
      _        <- failingSource.via(flow).runWith(Sink.ignore).recover { case _ => Done }
      readData <- ParquetStreams.fromParquet[Data].read(tempPathString).runWith(Sink.seq)
    } yield readData should have size numberOfSuccessfulWrites
  }

  it should "close viaParquet properly on downstream exception" in {
    val numberOfSuccessfulWrites = 25

    val failingSink = Sink.foreach[Data] {
      case Data(i, _) if i < numberOfSuccessfulWrites - 1 => ()
      case _                                              => throw new RuntimeException("test exception")
    }

    val flow = ParquetStreams
      .viaParquet[Data](tempPathString)
      .withWriteOptions(writeOptions)
      .build()

    for {
      _        <- Source(data).via(flow).runWith(failingSink).recover { case _ => Done }
      readData <- ParquetStreams.fromParquet[Data].read(tempPathString).runWith(Sink.seq)
    } yield readData should have size numberOfSuccessfulWrites
  }

  it should "close viaParquet properly on internal exception" in {
    val numberOfSuccessfulWrites = 25

    val parquetFlow = ParquetStreams
      .viaParquet[Data](tempPathString)
      .withWriteOptions(writeOptions)
      .withPostWriteHandler {
        case state if state.count >= numberOfSuccessfulWrites =>
          throw new RuntimeException("test exception")
        case _ =>
          ()
      }
      .build()

    for {
      _        <- Source(data).via(parquetFlow).runWith(Sink.ignore).recover { case _ => Done }
      readData <- ParquetStreams.fromParquet[Data].read(tempPathString).runWith(Sink.seq)
    } yield readData should have size numberOfSuccessfulWrites
  }

  it should "rotate files when max count is reached for each file" in {
    case class User(name: String, id: Int, id_part: String)

    val partitions   = 2
    val maxCount     = 1000
    val maxDuration  = 3.seconds
    val usersToWrite = maxCount * 5

    def genUser(id: Int) = User(
      name    = s"name_$id",
      id      = id,
      id_part = (id % partitions).toString
    )

    val flow = ParquetStreams
      .viaParquet[User](tempPathString)
      .withWriteOptions(writeOptions)
      .withMaxCount(maxCount = maxCount)
      .withMaxDuration(maxDuration = maxDuration)
      .withPartitionBy("id_part")
      .build()

    val users = ((1 to usersToWrite) map genUser).toList

    for {
      written <- Source(users).via(flow).runWith(Sink.seq)
      read    <- ParquetStreams.fromParquet[User].read(tempPathString).runWith(Sink.seq)
      files = fileSystem
        .listStatus(tempPath)
        .map(_.getPath)
        .toSeq
        .flatMap(fileSystem.listStatus(_).map(_.getPath).toSeq)
      readPartitioned <- Future.sequence( // generate the file lists in a partitioned form
        files.map(file => ParquetStreams.fromParquet[User].read(file.toString).runWith(Sink.seq))
      )
    } yield {
      written.length should be(usersToWrite)
      read.length should be(usersToWrite)

      val usersPerPartition = usersToWrite / partitions
      val filesPerPartition = (usersPerPartition / maxCount) + 1 // + 1 because the last file is partially filled
      files.length should be(filesPerPartition * partitions)

      readPartitioned.map(_.length).sorted.toList match {
        case a :: b :: c :: d :: e :: f :: Nil =>
          a should be(maxCount / partitions)
          b should be(maxCount / partitions)
          c should be(maxCount)
          d should be(maxCount)
          e should be(maxCount)
          f should be(maxCount)

        case _ =>
          fail("Unexpected partitioned data found")
      }
    }
  }

  it should "rotate files when max duration is reached for each file" in {
    case class User(name: String, id: Int, id_part: String)

    val partitions        = 2
    val intervals         = 5
    val maxCount          = 1000
    val maxDuration       = 1.second
    val usersToWrite      = 500
    val writeInterval     = maxDuration / intervals
    val usersPerInterval  = usersToWrite / intervals
    val filesPerPartition = 2

    def genUser(id: Int) = User(
      name    = s"name_$id",
      id      = id,
      id_part = (id % partitions).toString
    )

    val flow = ParquetStreams
      .viaParquet[User](tempPathString)
      .withWriteOptions(writeOptions)
      .withMaxCount(maxCount = maxCount)
      .withMaxDuration(maxDuration = maxDuration / filesPerPartition)
      .withPartitionBy("id_part")
      .build()

    val users = ((1 to usersToWrite) map genUser).toList

    for {
      written <- Source(users)
        .throttle(usersPerInterval, writeInterval)
        .via(flow)
        .take(usersToWrite)
        .runWith(Sink.seq)
      read <- ParquetStreams.fromParquet[User].read(tempPathString).runWith(Sink.seq)
      files = fileSystem
        .listStatus(tempPath)
        .map(_.getPath)
        .toSeq
        .flatMap(fileSystem.listStatus(_).map(_.getPath).toSeq)
      readPartitioned <- Future.sequence( // generate the file lists in a partitioned form
        files.map(file => ParquetStreams.fromParquet[User].read(file.toString).runWith(Sink.seq))
      )
    } yield {
      written.length should be(usersToWrite)
      read.length should be(usersToWrite)

      files.length should be(filesPerPartition * partitions)

      readPartitioned.map(_.length).toList match {
        case a :: b :: c :: d :: Nil =>
          a should be > 0
          b should be > 0
          c should be > 0
          d should be > 0

          val usersWritten = a + b + c + d
          usersWritten should be(usersToWrite)

        case _ =>
          fail("Unexpected partitioned data found")
      }
    }
  }

  override def afterAll(): Unit = {
    Await.ready(system.terminate(), Duration.Inf)
    super.afterAll()
  }

}
