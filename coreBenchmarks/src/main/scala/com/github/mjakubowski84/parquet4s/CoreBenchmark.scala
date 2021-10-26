package com.github.mjakubowski84.parquet4s

import org.openjdk.jmh.annotations.*

import java.io.IOException
import java.nio.file.attribute.BasicFileAttributes
import java.nio.file.{Path as NioPath, _}
import java.util.UUID
import java.util.concurrent.TimeUnit
import scala.collection.immutable
import scala.util.Random

case class Embedded(fraction: Double, text: String)
case class Record(i: Int, dict: String, embedded: Option[Embedded])

object CoreBenchmark {

  val Fractioner = 100.12
  val Dict       = List("a", "b", "c", "d")

  @State(Scope.Benchmark)
  class Dataset {

    // 512 * 1024
    @Param(Array("524288"))
    var datasetSize: Int                    = _
    var basePath: Path                      = _
    var records: immutable.Iterable[Record] = _

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
    }

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
    var dataset: Dataset = _
    var filePath: Path   = _

    def fetchDataset(dataset: Dataset): Unit = {
      this.dataset  = dataset
      this.filePath = dataset.basePath.append("file.parquet")
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
      ParquetWriter.of[Record].writeAndClose(filePath, dataset.records)

  }

  @State(Scope.Benchmark)
  class ReadState extends BaseState {

    @Setup(Level.Trial)
    def setup(dataset: Dataset): Unit = {
      fetchDataset(dataset)
      ParquetWriter.of[Record].writeAndClose(filePath, dataset.records)
    }

    @TearDown(Level.Trial)
    def clearDataset(): Unit = dataset.delete()

    @CompilerControl(CompilerControl.Mode.DONT_INLINE)
    def read(): Record =
      ParquetReader.as[Record].read(dataset.basePath).last
  }

}

@Warmup(iterations = 5, time = 500, timeUnit = TimeUnit.MILLISECONDS)
@Measurement(iterations = 12, time = 500, timeUnit = TimeUnit.MILLISECONDS)
class CoreBenchmark {

  import CoreBenchmark.*

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

}
