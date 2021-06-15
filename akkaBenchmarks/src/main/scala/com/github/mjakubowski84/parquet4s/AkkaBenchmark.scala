package com.github.mjakubowski84.parquet4s

import akka.actor.ActorSystem
import akka.stream.scaladsl.{Sink, Source}
import org.openjdk.jmh.annotations._

import java.io.IOException
import java.nio.file.attribute.BasicFileAttributes
import java.nio.file.{Path => NioPath, _}
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

  @State(Scope.Benchmark)
  class Dataset {

    // 1024 & 512 * 1024
    @Param(Array("1024", "524288"))
    var datasetSize: Int = _
    var basePath: Path = _
    var records: immutable.Iterable[Record] = _
    var actorSystem: ActorSystem = _

    @Setup(Level.Trial)
    def setup(): Unit = {
      basePath = Path(Files.createTempDirectory("benchmark")).append(datasetSize.toString)
      records = (1 to datasetSize).map { i =>
        Record(
          i = i,
          dict = Dict(Random.nextInt(Dict.size - 1)),
          embedded = if (i % 2 == 0) Some(Embedded(1.toDouble / Fractioner, UUID.randomUUID().toString))
          else None
        )
      }
      actorSystem = ActorSystem("benchmark")
    }

    @TearDown(Level.Trial)
    def tearDown(): Unit = Await.ready(actorSystem.terminate(), Duration.Inf)

    def delete(): NioPath = Files.walkFileTree(basePath.toNio, new FileVisitor[NioPath]() {
      override def preVisitDirectory(dir: NioPath, attrs: BasicFileAttributes): FileVisitResult = FileVisitResult.CONTINUE
      override def visitFile(file: NioPath, attrs: BasicFileAttributes): FileVisitResult = {
        Files.delete(file)
        FileVisitResult.CONTINUE
      }
      override def visitFileFailed(file: NioPath, exc: IOException): FileVisitResult = FileVisitResult.CONTINUE
      override def postVisitDirectory(dir: NioPath, exc: IOException): FileVisitResult = {
        Files.delete(dir)
        FileVisitResult.CONTINUE
      }
    })

  }

  trait BaseState {
    var dataset: Dataset = _
    var filePath: Path = _
    implicit var actorSystem: ActorSystem = _

    def fetchDataset(dataset: Dataset): Unit = {
      this.dataset = dataset
      this.filePath = dataset.basePath.append("file.parquet")
      this.actorSystem = dataset.actorSystem
    }
  }

  @State(Scope.Thread)
  class WriteState extends BaseState {

    @Setup(Level.Trial)
    def setup(dataset: Dataset): Unit = fetchDataset(dataset)

    @TearDown(Level.Invocation)
    def clearDataset(): Unit = dataset.delete()

    @CompilerControl(CompilerControl.Mode.DONT_INLINE)
    def write(): Unit =
      Await.ready(Source(dataset.records).runWith(ParquetStreams.toParquetSingleFile(filePath)), Duration.Inf)

    @CompilerControl(CompilerControl.Mode.DONT_INLINE)
    def akkaWritePartitioned(): Record =
        Await.result(
          Source(dataset.records)
            .via(ParquetStreams.viaParquet[Record](dataset.basePath).withPartitionBy(ColumnPath("dict")).build())
            .runWith(Sink.last)
          ,
          Duration.Inf
        )

  }

  @State(Scope.Benchmark)
  class ReadState extends BaseState {

    @Setup(Level.Trial)
    def setup(dataset: Dataset): Unit = {
      fetchDataset(dataset)
      ParquetWriter.writeAndClose(filePath, dataset.records)
    }

    @TearDown(Level.Trial)
    def clearDataset(): Unit = dataset.delete()

    @CompilerControl(CompilerControl.Mode.DONT_INLINE)
    def read(): Record =
      Await.result(ParquetStreams.fromParquet[Record].read(dataset.basePath).runWith(Sink.last), Duration.Inf)
  }

}

@Warmup(iterations = 5, time = 1, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 12, time = 1, timeUnit = TimeUnit.SECONDS)
class AkkaBenchmark {

  import AkkaBenchmark._

  @Benchmark
  @BenchmarkMode(Array(Mode.AverageTime))
  @OutputTimeUnit(TimeUnit.MILLISECONDS)
  def write(state: WriteState): Unit =
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
