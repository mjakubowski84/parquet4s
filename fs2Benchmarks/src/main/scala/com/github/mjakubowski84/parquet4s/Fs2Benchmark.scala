package com.github.mjakubowski84.parquet4s

import cats.effect.IO
import cats.effect.unsafe.{IORuntime, IORuntimeConfig, Scheduler}
import fs2.{Chunk, Stream}
import org.openjdk.jmh.annotations.*

import java.io.IOException
import java.nio.file.attribute.BasicFileAttributes
import java.nio.file.{Path as NioPath, *}
import java.util.UUID
import java.util.concurrent.{Executors, TimeUnit}
import scala.collection.immutable
import scala.concurrent.ExecutionContext
import scala.util.Random

case class Embedded(fraction: Double, text: String)
case class Record(i: Int, dict: String, embedded: Option[Embedded])

case class LargeRecord(
    i: Int,
    dict: String,
    embedded: Option[Embedded],
    a: String,
    b: String,
    c: String,
    d: String,
    e: String
)

object Fs2Benchmark {

  private val Fractioner = 100.12
  private val Dict       = List("a", "b", "c", "d")

  @State(Scope.Benchmark)
  class Dataset {

    // 512 * 1024
    @Param(Array("524288"))
    var datasetSize: Int                              = _
    var basePath: Path                                = _
    var records: immutable.Iterable[Record]           = _
    var largeRecords: immutable.Iterable[LargeRecord] = _
    var ioRuntime: IORuntime                          = _

    @Setup(Level.Trial)
    def setup(): Unit = {
      System.setProperty("cats.effect.stackTracingMode", "disabled")
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
      largeRecords = (1 to datasetSize).map { i =>
        LargeRecord(
          i    = i,
          dict = Dict(Random.nextInt(Dict.size - 1)),
          embedded =
            if (i % 2 == 0) Some(Embedded(1.toDouble / Fractioner, UUID.randomUUID().toString))
            else None,
          a = Dict(Random.nextInt(Dict.size - 1)),
          b = Dict(Random.nextInt(Dict.size - 1)),
          c = Dict(Random.nextInt(Dict.size - 1)),
          d = Dict(Random.nextInt(Dict.size - 1)),
          e = Dict(Random.nextInt(Dict.size - 1))
        )
      }
      // using single thread in order to not measure thread syncing
      // using the same execution context for blocking ops in order to avoid unnecessary thread switching
      val threadPool          = Executors.newSingleThreadExecutor()
      val executionContext    = ExecutionContext.fromExecutor(threadPool)
      val schedulerThreadPool = Executors.newSingleThreadScheduledExecutor()
      val scheduler           = Scheduler.fromScheduledExecutor(schedulerThreadPool)
      ioRuntime = IORuntime(
        compute   = executionContext,
        blocking  = executionContext,
        scheduler = scheduler,
        shutdown  = () => { threadPool.shutdown(); schedulerThreadPool.shutdown() },
        config    = IORuntimeConfig()
      )
    }

    @TearDown(Level.Trial)
    def tearDown(): Unit = ioRuntime.shutdown()

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
    var dataset: Dataset              = _
    var filePath: Path                = _
    implicit var ioRuntime: IORuntime = _

    def fetchDataset(dataset: Dataset): Unit = {
      this.dataset   = dataset
      this.filePath  = dataset.basePath.append("file.parquet")
      this.ioRuntime = dataset.ioRuntime
    }
  }

  @State(Scope.Thread)
  class WriteState extends BaseState {

    var operation: IO[Unit]        = _
    var chunks: Seq[Chunk[Record]] = _

    @Setup(Level.Trial)
    def setup(dataset: Dataset): Unit = {
      fetchDataset(dataset)
      chunks = Stream.iterable(dataset.records).chunkN(16).compile.toVector
      operation = Stream
        .iterable(chunks)
        .unchunks
        .through(parquet.writeSingleFile[IO].of[Record].write(filePath))
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

    var operation: IO[Record]      = _
    var chunks: Seq[Chunk[Record]] = _

    @Setup(Level.Trial)
    def setup(dataset: Dataset): Unit = {
      fetchDataset(dataset)
      chunks = Stream.iterable(dataset.records).chunkN(16).compile.toVector
      operation = Stream
        .iterable(chunks)
        .unchunks
        .through(parquet.viaParquet[IO].of[Record].partitionBy(ColumnPath("dict")).write(dataset.basePath))
        .compile
        .lastOrError
    }

    @TearDown(Level.Invocation)
    def clearDataset(): Unit = dataset.delete()

    @CompilerControl(CompilerControl.Mode.DONT_INLINE)
    def writePartitioned(): Record =
      operation.unsafeRunSync()
  }

  @State(Scope.Thread)
  class WritePartitionedLargeRecordState extends BaseState {
    var operation: IO[LargeRecord]      = _
    var chunks: Seq[Chunk[LargeRecord]] = _

    @Setup(Level.Trial)
    def setup(dataset: Dataset): Unit = {
      fetchDataset(dataset)
      chunks = Stream.iterable(dataset.largeRecords).chunkN(16).compile.toVector
      operation = Stream
        .iterable(chunks)
        .unchunks
        .through(
          parquet
            .viaParquet[IO]
            .of[LargeRecord]
            .partitionBy(ColumnPath("a"), ColumnPath("b"), ColumnPath("c"), ColumnPath("d"), ColumnPath("e"))
            .write(dataset.basePath)
        )
        .compile
        .lastOrError
    }

    @TearDown(Level.Invocation)
    def clearDataset(): Unit = dataset.delete()

    @CompilerControl(CompilerControl.Mode.DONT_INLINE)
    def writePartitionedLargeRecord(): LargeRecord =
      operation.unsafeRunSync()
  }

  @State(Scope.Benchmark)
  class ReadState extends BaseState {

    var operation: IO[Record] = _

    @Setup(Level.Trial)
    def setup(dataset: Dataset): Unit = {
      fetchDataset(dataset)
      ParquetWriter.of[Record].writeAndClose(filePath, dataset.records)
      operation = parquet
        .fromParquet[IO]
        .as[Record]
        .read(dataset.basePath)
        .compile
        .lastOrError
    }

    @TearDown(Level.Trial)
    def clearDataset(): Unit =
      dataset.delete()

    @CompilerControl(CompilerControl.Mode.DONT_INLINE)
    def read(): Record =
      operation.unsafeRunSync()
  }

}

@Warmup(iterations = 5, time = 1, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 12, time = 1, timeUnit = TimeUnit.SECONDS)
class Fs2Benchmark {

  import Fs2Benchmark.*

  @Benchmark
  @BenchmarkMode(Array(Mode.AverageTime))
  @OutputTimeUnit(TimeUnit.MILLISECONDS)
  @Fork(jvmArgsAppend = Array("-Dcats.effect.stackTracingMode=disabled"))
  def write(state: WriteState): Unit =
    state.write()

  @Benchmark
  @BenchmarkMode(Array(Mode.AverageTime))
  @OutputTimeUnit(TimeUnit.MILLISECONDS)
  @Fork(jvmArgsAppend = Array("-Dcats.effect.stackTracingMode=disabled"))
  def read(state: ReadState): Record =
    state.read()

  @Benchmark
  @BenchmarkMode(Array(Mode.AverageTime))
  @OutputTimeUnit(TimeUnit.MILLISECONDS)
  @Fork(jvmArgsAppend = Array("-Dcats.effect.stackTracingMode=disabled"))
  def writePartitioned(state: WritePartitionedState): Record =
    state.writePartitioned()

  @Benchmark
  @BenchmarkMode(Array(Mode.AverageTime))
  @OutputTimeUnit(TimeUnit.MILLISECONDS)
  @Fork(jvmArgsAppend = Array("-Dcats.effect.stackTracingMode=disabled"))
  def writePartitionedLargeRecord(state: WritePartitionedLargeRecordState): LargeRecord =
    state.writePartitionedLargeRecord()
}
