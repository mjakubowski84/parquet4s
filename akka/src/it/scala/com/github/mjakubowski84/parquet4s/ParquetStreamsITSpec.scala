package com.github.mjakubowski84.parquet4s

import akka.Done
import akka.actor.ActorSystem
import akka.stream.scaladsl.{Sink, Source}
import org.apache.hadoop.fs.Path
import org.apache.parquet.hadoop.metadata.CompressionCodecName
import org.apache.parquet.schema.MessageType
import org.scalatest.flatspec.AsyncFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.{BeforeAndAfter, BeforeAndAfterAll, Inspectors}
import org.slf4j.{Logger, LoggerFactory}

import java.sql.Timestamp
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

  override val logger: Logger = LoggerFactory.getLogger(this.getClass)
  implicit val system: ActorSystem = ActorSystem()

  val writeOptions: ParquetWriter.Options = ParquetWriter.Options(
    compressionCodecName = CompressionCodecName.SNAPPY,
    pageSize = 512,
    rowGroupSize = 4 * 512,
    hadoopConf = configuration
  )

  val count: Int = 4 * writeOptions.rowGroupSize
  val dict: Seq[String] = Vector("a", "b", "c", "d")
  val data: LazyList[Data] = LazyList
    .range(start = 0L, end = count, step = 1L)
    .map(i => Data(i = i, s = dict(Random.nextInt(4))))

  def read[T : ParquetRecordDecoder](path: Path, filter: Filter = Filter.noopFilter): Future[immutable.Seq[T]] =
    ParquetStreams.fromParquet[T].withFilter(filter).read(path.toString).runWith(Sink.seq)

  before {
    clearTemp()
  }

  "ParquetStreams" should "write single file and read it correctly" in {
    val outputFileName = "data.parquet"

    val write = () => Source(data).runWith(ParquetStreams.toParquetSingleFile(
      path = s"$tempPath/$outputFileName",
      options = writeOptions
    ))

    for {
      _ <- write()
      files <- filesAtPath(tempPath, configuration)
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
      files <- filesAtPath(tempPath, configuration)
      readData <- read[Data](tempPath)
    } yield {
      files should be(empty)
      readData should be(empty)
    }
  }

  it should "be able to filter files during reading" in {
    val outputFileName = "data.parquet"

    val write = () => Source(data).runWith(ParquetStreams.toParquetSingleFile(
      path = s"$tempPath/$outputFileName",
      options = writeOptions
    ))

    for {
      _ <- write()
      readData <- read[Data](tempPath, filter = Col("s") === "a")
    } yield {
      readData should not be empty
      forAll(readData) { _.s should be("a") }
    }
  }

  it should "be able to read partitioned data" in {
    val outputFileName = "data.parquet"

    val write = (a: String, b: String) => Source(data).runWith(ParquetStreams.toParquetSingleFile(
      path = s"$tempPath/a=$a/b=$b/$outputFileName",
      options = writeOptions
    ))

    for {
      _ <- write("A", "1")
      _ <- write("A", "2")
      _ <- write("B", "1")
      _ <- write("B", "2")
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

    val write = (midPath: String) => Source(data).runWith(ParquetStreams.toParquetSingleFile(
      path = s"$tempPath/$midPath/$outputFileName",
      options = writeOptions
    ))

    for {
      _ <- write("a=A/b=1")
      _ <- write("a=A/b=2")
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

    val write = (midPath: String) => Source(data).runWith(ParquetStreams.toParquetSingleFile(
      path = s"$tempPath/$midPath/$outputFileName",
      options = writeOptions
    ))

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

    val write = () => Source(data).runWith(ParquetStreams.toParquetSingleFile(
      path = s"$tempPath/$outputFileName",
      options = writeOptions
    ))

    for {
      _ <- write()
      files <- filesAtPath(tempPath, configuration)
      readData <-  ParquetStreams.fromParquet[S].withProjection.read(tempPathString).runWith(Sink.seq)
    } yield {
      files should be(List(outputFileName))
      readData should have size count
      readData should contain theSameElementsInOrderAs data.map(d => S(d.s))
    }
  }

  it should "write and read partitioned data" in {

    case class User(name: String, address: Address)
    case class Address(street: Street, country: String, postCode: String)
    case class Street(name: String, more: String)

    val flow = ParquetStreams.viaParquet[User](tempPathString)
      .withWriteOptions(writeOptions)
      .withMaxCount(writeOptions.rowGroupSize)
      .withMaxDuration(100.millis)
      .withPartitionBy(Col("address.country"), Col("address.postCode"))
      .build()

    val users = Seq(
      User(name = "John", address = Address(street = Street("Broad St", "12"), country = "ABC", postCode = "123456"))
    ).toList

    val firstPartitionPath = tempPath.suffix("/address.country=ABC")
    val secondPartitionPath = firstPartitionPath.suffix("/address.postCode=123456")

    for {
      writtenData <- Source(users).via(flow).runWith(Sink.seq)
      readData <- ParquetStreams.fromParquet[User].read(tempPathString).runWith(Sink.seq)
      rootFiles = fileSystem.listStatus(tempPath).map(_.getPath).toSeq
      firstPartitionFiles = fileSystem.listStatus(firstPartitionPath).map(_.getPath).toSeq
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

    val maxCount = 10
    val usersToWrite = 33
    val partitions = 2
    val lastToFlushOnDemand = 2

    def genUser(id: Int) = User(s"name_$id", id, (id % partitions).toString) // Two partitions : even and odd `id`

    // Mimics a metrics library
    var metrics = Vector.empty[(Long, Int)]
    def gauge(count: Long, userId: Int): Unit = metrics :+= (count, userId)

    val flow = ParquetStreams.viaParquet[User](tempPathString)
      .withWriteOptions(writeOptions)
      .withMaxCount(maxCount)
      .withPartitionBy(Col("id_part"))
      .withPostWriteHandler { state =>
        gauge(state.count, state.lastProcessed.id)                       // use-case 1 : monitoring internal counter
        if (state.lastProcessed.id > usersToWrite - lastToFlushOnDemand) // use-case 2 : on demand flush. e.g: the last two records must be in separate files
          state.flush()
      }
      .build()

    val users = ((1 to usersToWrite) map genUser).toList

    for {
      writtenData <- Source(users).via(flow).runWith(Sink.seq)
      readData <- ParquetStreams.fromParquet[User].read(tempPathString).runWith(Sink.seq)
      rootFiles = fileSystem.listStatus(tempPath).map(_.getPath).toSeq
      files = rootFiles.flatMap(p => fileSystem.listStatus(p).map(_.getPath).toSeq)
      readDataPartitioned <- Future.sequence( // generate the file lists in a partitioned form
        files.map(file =>
          ParquetStreams.fromParquet[User].read(file.toString).runWith(Sink.seq)
        ))
    } yield {
      every(files.map(_.getName)) should endWith(".snappy.parquet")
      writtenData should be(users)

      readData should have size users.size
      readData should contain allElementsOf users

      files should have size (((usersToWrite / maxCount) * partitions) + 1 + lastToFlushOnDemand) // 9 == ( 33 / 10 ) * 2 + 1[remainder] + 2)

      // the counter is flushed 3 times due to max-count being reached
      val completeCounts = Seq.fill(usersToWrite / maxCount) { 0 until maxCount }
        .flatten
        .zipWithIndex
        .map { case (count, id) => (count + 1) -> (id + 1)}

      // the counter is flushed twice due to on-demand calls
      val onDemandCallsCounts = Seq(
        1,
        2, // first on-demand flush
        1  // second on-demand flush
      ).zipWithIndex.map { case (count, i) => count -> (completeCounts.length + i + 1)}

      metrics should be (completeCounts ++ onDemandCallsCounts)

      val (remainder, full) = readDataPartitioned.partition(_.head.id >= usersToWrite - lastToFlushOnDemand)
      every(full) should have size (maxCount / partitions)                              // == 5 records in completed files
      remainder should have size (usersToWrite - (usersToWrite / maxCount) * maxCount)  // == 3 files are flushed prematurely
      every(remainder) should have size 1                                               // == 1 record in prematurely flushed files
    }
  }

  it should "write and read data partitioned by all fields of case class" in {

    case class User(name: String, address: Address)
    case class Address(postcode: String, country: String)

    val flow = ParquetStreams.viaParquet[User](tempPathString)
      .withWriteOptions(writeOptions)
      .withMaxCount(writeOptions.rowGroupSize)
      .withMaxDuration(100.millis)
      .withPartitionBy(Col("address.country"), Col("address.postcode"))
      .build()

    val users = Seq(
      User(name = "John", address = Address("123456", "ABC"))
    ).toList

    for {
      writtenData <- Source(users).via(flow).runWith(Sink.seq)
      readData <- ParquetStreams.fromParquet[User].read(tempPathString).runWith(Sink.seq)
    } yield {
      writtenData should be(users)
      readData should be(users)
    }
  }

  it should "fail to write a case class when partitioning consumes all its fields" in {

    case class User(name: String)

    val flow = ParquetStreams.viaParquet[User](tempPathString)
      .withWriteOptions(writeOptions)
      .withMaxCount(writeOptions.rowGroupSize)
      .withMaxDuration(100.millis)
      .withPartitionBy(Col("name"))
      .build()

    val users = Seq(User(name = "John")).toList

    val fut = Source(users).via(flow).runWith(Sink.ignore)
    recoverToSucceededIf[org.apache.parquet.schema.InvalidSchemaException](fut)
  }

  it should "write and read data using partitioning flow but with no partition defined" in {

    case class User(name: String, address: Address)
    case class Address(postcode: String, country: String)

    val flow = ParquetStreams.viaParquet[User](tempPathString)
      .withWriteOptions(writeOptions)
      .withMaxCount(writeOptions.rowGroupSize)
      .withMaxDuration(100.millis)
      .build()

    val users = Seq(
      User(name = "John", address = Address("123456", "ABC")),
      User(name = "Ben", address = Address("111111", "CDE")),
      User(name = "Cynthia", address = Address("7654321", "XYZ"))
    ).toList

    for {
      writtenData <- Source(users).via(flow).runWith(Sink.seq)
      readData <- ParquetStreams.fromParquet[User].read(tempPathString).runWith(Sink.seq)
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

    val flow = ParquetStreams.viaParquet[RowParquetRecord](tempPathString)
      .withWriteOptions(writeOptions)
      .withMaxCount(writeOptions.rowGroupSize)
      .withMaxDuration(100.millis)
      .withPartitionBy(Col("address.country"), Col("address.postCode"))
      .build()

    val genericUsers = Seq(
      RowParquetRecord.empty
          .add("name", "John")
          .add(Col("address.street.name"), "Broad St")
          .add(Col("address.street.more"), "12")
          .add(Col("address.country"), "ABC")
          .add(Col("address.postCode"), "123456")
    ).toList

    val expectedUsers = Seq(
      User(name = "John", address = Address(street = Street("Broad St", "12"), country = "ABC", postCode = "123456"))
    )

    val firstPartitionPath = tempPath.suffix("/address.country=ABC")
    val secondPartitionPath = firstPartitionPath.suffix("/address.postCode=123456")

    for {
      writtenData <- Source(genericUsers).via(flow).runWith(Sink.seq)
      readData <- ParquetStreams.fromParquet[RowParquetRecord].read(tempPathString).runWith(Sink.seq)
      rootFiles = fileSystem.listStatus(tempPath).map(_.getPath).toSeq
      firstPartitionFiles = fileSystem.listStatus(firstPartitionPath).map(_.getPath).toSeq
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
      case _ => throw new RuntimeException("test exception")
    }

    val flow = ParquetStreams.viaParquet[Data](tempPathString)
      .withWriteOptions(writeOptions)
      .build()

    for {
      _ <- failingSource.via(flow).runWith(Sink.ignore).recover { case _ => Done }
      readData <- ParquetStreams.fromParquet[Data].read(tempPathString).runWith(Sink.seq)
    } yield
      readData should have size numberOfSuccessfulWrites
  }

  it should "close viaParquet properly on downstream exception" in {
    val numberOfSuccessfulWrites = 25

    val failingSink = Sink.foreach[Data] {
      case Data(i, _) if i < numberOfSuccessfulWrites - 1 => ()
      case _ => throw new RuntimeException("test exception")
    }

    val flow = ParquetStreams.viaParquet[Data](tempPathString)
      .withWriteOptions(writeOptions)
      .build()

    for {
      _ <- Source(data).via(flow).runWith(failingSink).recover { case _ => Done }
      readData <- ParquetStreams.fromParquet[Data].read(tempPathString).runWith(Sink.seq)
    } yield
      readData should have size numberOfSuccessfulWrites
  }

  it should "close viaParquet properly on internal exception" in {
    val numberOfSuccessfulWrites = 25

    val parquetFlow = ParquetStreams.viaParquet[Data](tempPathString)
      .withWriteOptions(writeOptions)
      .withPostWriteHandler {
        case state if state.count >= numberOfSuccessfulWrites =>
          throw new RuntimeException("test exception")
        case _ =>
          ()
      }
      .build()

    for {
      _ <- Source(data).via(parquetFlow).runWith(Sink.ignore).recover { case _ => Done }
      readData <- ParquetStreams.fromParquet[Data].read(tempPathString).runWith(Sink.seq)
    } yield
      readData should have size numberOfSuccessfulWrites
  }

  override def afterAll(): Unit = {
    Await.ready(system.terminate(), Duration.Inf)
    super.afterAll()
  }

}
