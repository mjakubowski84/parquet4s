package com.github.mjakubowski84.parquet4s

import cats.effect.{Blocker, ContextShift, IO, Timer}
import fs2.Stream
import org.openjdk.jmh.annotations._

import java.io.IOException
import java.nio.file._
import java.nio.file.attribute.BasicFileAttributes
import java.util.UUID
import java.util.concurrent.{ExecutorService, Executors, TimeUnit}
import scala.collection.immutable
import scala.concurrent.ExecutionContext
import scala.util.Random

case class Embedded(fraction: Double, text: String)
case class Record(i: Int, dict: String, embedded: Option[Embedded])


object Fs2Benchmark {

  val Fractioner = 100.12
  val Dict = List("a", "b", "c", "d")

  @State(Scope.Benchmark)
  class Dataset {

    // 512 * 1024
    @Param(Array("524288"))
    var datasetSize: Int = _
    var basePath: String = _
    var records: immutable.Iterable[Record] = _
    var threadPool: ExecutorService = _
    var executionContext: ExecutionContext = _
    var blocker: Blocker = _
    var contextShift: ContextShift[IO] = _
    var timer: Timer[IO] = _

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
      // using single thread in order to not measure thread syncing
      threadPool = Executors.newSingleThreadExecutor()
      executionContext = ExecutionContext.fromExecutor(threadPool)
      // using the same execution context in order to avoid unnecessary thread switching
      blocker = Blocker.liftExecutionContext(executionContext)
      contextShift = IO.contextShift(executionContext)
      timer = IO.timer(executionContext)
    }

    @TearDown(Level.Trial)
    def tearDown(): Unit =
      threadPool.shutdown()

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
    implicit var contextShift: ContextShift[IO] = _
    implicit var timer: Timer[IO] = _

    def fetchDataset(dataset: Dataset): Unit = {
      this.dataset = dataset
      this.filePath = dataset.basePath + "/file.parquet"
      this.contextShift = dataset.contextShift
      this.timer = dataset.timer
    }
  }

  @State(Scope.Thread)
  class WriteState extends BaseState {

    var operation: IO[Unit] = _

    @Setup(Level.Trial)
    def setup(dataset: Dataset): Unit = {
      fetchDataset(dataset)
      operation = Stream
        .iterable(dataset.records)
        .through(parquet.writeSingleFile[IO, Record](dataset.blocker, filePath))
        .compile
        .drain
    }

    @TearDown(Level.Invocation)
    def clearDataset(): Unit = dataset.delete()

    @CompilerControl(CompilerControl.Mode.DONT_INLINE)
    def write(): Unit =
      operation.unsafeRunSync()

  }

  @State(Scope.Thread)
  class WritePartitionedState extends BaseState {

    var operation: IO[Record] = _

    @Setup(Level.Trial)
    def setup(dataset: Dataset): Unit = {
      fetchDataset(dataset)
      operation = Stream
        .iterable(dataset.records)
        .through(parquet.viaParquet[IO, Record].partitionBy("dict").write(dataset.blocker, dataset.basePath))
        .compile
        .lastOrError
    }

    @TearDown(Level.Invocation)
    def clearDataset(): Unit = dataset.delete()

    @CompilerControl(CompilerControl.Mode.DONT_INLINE)
    def writePartitioned(): Record =
      operation.unsafeRunSync()

  }

  @State(Scope.Benchmark)
  class ReadState extends BaseState {

    var operation: IO[Record] = _

    @Setup(Level.Trial)
    def setup(dataset: Dataset): Unit = {
      fetchDataset(dataset)
      ParquetWriter.writeAndClose(filePath, dataset.records)
      operation = parquet
        .fromParquet[IO, Record]
        .read(dataset.blocker, dataset.basePath)
        .compile
        .lastOrError
    }

    @TearDown(Level.Trial)
    def clearDataset(): Unit = {
      dataset.delete()
    }

    @CompilerControl(CompilerControl.Mode.DONT_INLINE)
    def read(): Record =
      operation.unsafeRunSync()
  }

}

@Warmup(iterations = 5, time = 1, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 12, time = 1, timeUnit = TimeUnit.SECONDS)
class Fs2Benchmark {

  import Fs2Benchmark._

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
  def writePartitioned(state: WritePartitionedState): Record =
    state.writePartitioned()

}
