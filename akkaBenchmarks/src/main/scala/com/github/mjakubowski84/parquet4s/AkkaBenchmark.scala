package com.github.mjakubowski84.parquet4s

import akka.Done
import akka.actor.ActorSystem
import akka.stream.ActorAttributes
import akka.stream.scaladsl.{Keep, Sink, Source}
import com.typesafe.config.ConfigFactory
import org.openjdk.jmh.annotations._

import java.io.IOException
import java.nio.file.attribute.BasicFileAttributes
import java.nio.file._
import java.util.UUID
import java.util.concurrent.TimeUnit
import scala.collection.immutable
import scala.concurrent.Await
import scala.concurrent.duration.Duration
import scala.util.Random

case class Embedded(fraction: Double, text: String)
case class Record(i: Int, dict: String, embedded: Option[Embedded])


object AkkaBenchmark {

  val Fractioner = 100.12
  val Dict = List("a", "b", "c", "d")
  val Dispatcher = "akka.actor.single-thread-dispatcher"

  @State(Scope.Benchmark)
  class Dataset {

    // 512 * 1024
    @Param(Array("524288"))
    var datasetSize: Int = _
    var basePath: String = _
    var records: immutable.Iterable[Record] = _
    var actorSystem: ActorSystem = _

    @Setup(Level.Trial)
    def setup(): Unit = {
      basePath = Files.createTempDirectory("benchmark").resolve(datasetSize.toString).toString
      records = (1 to datasetSize).map { i =>
        Record(
          i = i,
          dict = Dict(Random.nextInt(Dict.size - 1)),
          embedded = if (i % 2 == 0) Some(Embedded(1.toDouble / Fractioner, UUID.randomUUID().toString))
          else None
        )
      }
      actorSystem = ActorSystem("Benchmark", ConfigFactory.parseString(
        """
            akka.actor.single-thread-dispatcher {
                type = PinnedDispatcher
                executor = "thread-pool-executor"
                thread-pool-executor {
                    fixed-pool-size = 1
                }
            }
            """
      ))
    }

    @TearDown(Level.Trial)
    def tearDown(): Unit = Await.ready(actorSystem.terminate(), Duration.Inf)

    def delete(): Path = Files.walkFileTree(Paths.get(basePath), new FileVisitor[Path]() {
      override def preVisitDirectory(dir: Path, attrs: BasicFileAttributes): FileVisitResult = FileVisitResult.CONTINUE
      override def visitFile(file: Path, attrs: BasicFileAttributes): FileVisitResult = {
        Files.delete(file)
        FileVisitResult.CONTINUE
      }
      override def visitFileFailed(file: Path, exc: IOException): FileVisitResult = FileVisitResult.CONTINUE
      override def postVisitDirectory(dir: Path, exc: IOException): FileVisitResult = {
        Files.delete(dir)
        FileVisitResult.CONTINUE
      }
    })

  }

  trait BaseState {
    var dataset: Dataset = _
    var filePath: String = _
    implicit var actorSystem: ActorSystem = _

    def fetchDataset(dataset: Dataset): Unit = {
      this.dataset = dataset
      this.filePath = dataset.basePath + "/file.parquet"
      this.actorSystem = dataset.actorSystem
    }
  }

  @State(Scope.Thread)
  class WriteState extends BaseState {

    private def writeGraph =
      Source(dataset.records)
        .toMat(ParquetStreams.toParquetSingleFile(filePath))(Keep.right)
        .withAttributes(ActorAttributes.dispatcher(Dispatcher))

    private def writePartitionedGraph =
      Source(dataset.records)
        .via(ParquetStreams.viaParquet[Record](dataset.basePath).withPartitionBy("dict").build())
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
    def akkaWritePartitioned(): Record =
      Await.result(writePartitionedGraph.run(), Duration.Inf)

  }

  @State(Scope.Benchmark)
  class ReadState extends BaseState {

    private def readGraph =
      ParquetStreams
        .fromParquet[Record]
        .read(dataset.basePath)
        .toMat(Sink.last)(Keep.right)
        .withAttributes(ActorAttributes.dispatcher(Dispatcher))

    @Setup(Level.Trial)
    def setup(dataset: Dataset): Unit = {
      fetchDataset(dataset)
      ParquetWriter.writeAndClose(filePath, dataset.records)
    }

    @TearDown(Level.Trial)
    def clearDataset(): Unit = dataset.delete()

    @CompilerControl(CompilerControl.Mode.DONT_INLINE)
    def read(): Record = Await.result(readGraph.run(), Duration.Inf)
  }

}

@Warmup(iterations = 5, time = 1, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 12, time = 1, timeUnit = TimeUnit.SECONDS)
class AkkaBenchmark {

  import AkkaBenchmark._

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
    state.akkaWritePartitioned()

}
