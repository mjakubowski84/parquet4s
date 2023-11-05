package com.github.mjakubowski84.parquet4s

import com.github.mjakubowski84.parquet4s.ScalaCompat.Done
import com.github.mjakubowski84.parquet4s.ScalaCompat.actor.ActorSystem
import com.github.mjakubowski84.parquet4s.ScalaCompat.stream.ActorAttributes
import com.github.mjakubowski84.parquet4s.ScalaCompat.stream.scaladsl.{Keep, Sink, Source}
import com.typesafe.config.{Config, ConfigFactory}
import org.openjdk.jmh.annotations.*

import java.io.IOException
import java.nio.file.attribute.BasicFileAttributes
import java.nio.file.{Path as NioPath, *}
import java.util.UUID
import java.util.concurrent.TimeUnit
import scala.collection.immutable
import scala.concurrent.Await
import scala.concurrent.duration.Duration
import scala.util.Random

case class Embedded(fraction: Double, text: String)
case class Record(i: Int, dict: String, embedded: Option[Embedded])

object AkkaPekkoBenchmark {
  val isAkkaSystem: Boolean = Done.getClass.getPackage.getName == "akka"
  val Fractioner            = 100.12
  val Dict: Seq[String]     = List("a", "b", "c", "d")
  val Dispatcher: String =
    if (isAkkaSystem) "akka.actor.single-thread-dispatcher" else "pekko.actor.single-thread-dispatcher"

  @State(Scope.Benchmark)
  class Dataset {

    // 512 * 1024
    @Param(Array("524288"))
    var datasetSize: Int                    = _
    var basePath: Path                      = _
    var records: immutable.Iterable[Record] = _
    var actorSystem: ActorSystem            = _
    val actorSystemConfig: Config = if (isAkkaSystem) {
      ConfigFactory.parseString(
        """
          akka.actor.single-thread-dispatcher {
              type = PinnedDispatcher
              executor = "thread-pool-executor"
              thread-pool-executor {
                  fixed-pool-size = 1
              }
          }
        """
      )
    } else {
      ConfigFactory.parseString(
        """
          pekko.actor.single-thread-dispatcher {
              type = PinnedDispatcher
              executor = "thread-pool-executor"
              thread-pool-executor {
                  fixed-pool-size = 1
              }
          }
        """
      )
    }

    @Setup(Level.Trial)
    def setup(): Unit = {
      basePath = Path(Files.createTempDirectory("benchmark")).append(datasetSize.toString)
      records = (1 to datasetSize).map { i =>
        Record(
          i    = i,
          dict = Dict(Random.nextInt(Dict.size - 1)),
          embedded =
            if (i % 2 == 0) Some(Embedded(1.toDouble / Fractioner, UUID.randomUUID().toString))
            else None
        )
      }
      actorSystem = ActorSystem(
        "Benchmark",
        actorSystemConfig
      )
    }

    @TearDown(Level.Trial)
    def tearDown(): Unit = Await.ready(actorSystem.terminate(), Duration.Inf)

    def delete(): NioPath = Files.walkFileTree(
      basePath.toNio,
      new FileVisitor[NioPath]() {
        override def preVisitDirectory(dir: NioPath, attrs: BasicFileAttributes): FileVisitResult =
          FileVisitResult.CONTINUE
        override def visitFile(file: NioPath, attrs: BasicFileAttributes): FileVisitResult = {
          Files.delete(file)
          FileVisitResult.CONTINUE
        }
        override def visitFileFailed(file: NioPath, exc: IOException): FileVisitResult = FileVisitResult.CONTINUE
        override def postVisitDirectory(dir: NioPath, exc: IOException): FileVisitResult = {
          Files.delete(dir)
          FileVisitResult.CONTINUE
        }
      }
    )

  }

  trait BaseState {
    var dataset: Dataset                  = _
    var filePath: Path                    = _
    implicit var actorSystem: ActorSystem = _

    def fetchDataset(dataset: Dataset): Unit = {
      this.dataset     = dataset
      this.filePath    = dataset.basePath.append("file.parquet")
      this.actorSystem = dataset.actorSystem
    }
  }

  @State(Scope.Thread)
  class WriteState extends BaseState {

    private def writeGraph =
      Source(dataset.records)
        .toMat(ParquetStreams.toParquetSingleFile.of[Record].write(filePath))(Keep.right)
        .withAttributes(ActorAttributes.dispatcher(Dispatcher))

    private def writePartitionedGraph =
      Source(dataset.records)
        .via(ParquetStreams.viaParquet.of[Record].partitionBy(ColumnPath("dict")).write(dataset.basePath))
        .toMat(Sink.last)(Keep.right)
        .withAttributes(ActorAttributes.dispatcher(Dispatcher))

    @Setup(Level.Trial)
    def setup(dataset: Dataset): Unit = fetchDataset(dataset)

    @TearDown(Level.Invocation)
    def clearDataset(): Unit = dataset.delete()

    @CompilerControl(CompilerControl.Mode.DONT_INLINE)
    def write(): Done =
      Await.result(writeGraph.run(), Duration.Inf)

    @CompilerControl(CompilerControl.Mode.DONT_INLINE)
    def akkaPekkoWritePartitioned(): Record =
      Await.result(writePartitionedGraph.run(), Duration.Inf)

  }

  @State(Scope.Benchmark)
  class ReadState extends BaseState {

    private def readGraph =
      ParquetStreams.fromParquet
        .as[Record]
        .read(dataset.basePath)
        .toMat(Sink.last)(Keep.right)
        .withAttributes(ActorAttributes.dispatcher(Dispatcher))

    @Setup(Level.Trial)
    def setup(dataset: Dataset): Unit = {
      fetchDataset(dataset)
      ParquetWriter.of[Record].writeAndClose(filePath, dataset.records)
    }

    @TearDown(Level.Trial)
    def clearDataset(): Unit = dataset.delete()

    @CompilerControl(CompilerControl.Mode.DONT_INLINE)
    def read(): Record = Await.result(readGraph.run(), Duration.Inf)
  }

}

@Warmup(iterations = 5, time = 1, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 12, time = 1, timeUnit = TimeUnit.SECONDS)
class AkkaPekkoBenchmark {

  import AkkaPekkoBenchmark.*

  @Benchmark
  @BenchmarkMode(Array(Mode.AverageTime))
  @OutputTimeUnit(TimeUnit.MILLISECONDS)
  def write(state: WriteState): Done =
    state.write()

  @Benchmark
  @BenchmarkMode(Array(Mode.AverageTime))
  @OutputTimeUnit(TimeUnit.MILLISECONDS)
  def read(state: ReadState): Record =
    state.read()

  @Benchmark
  @BenchmarkMode(Array(Mode.AverageTime))
  @OutputTimeUnit(TimeUnit.MILLISECONDS)
  def writePartitioned(state: WriteState): Record =
    state.akkaPekkoWritePartitioned()

}
