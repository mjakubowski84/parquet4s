package com.github.mjakubowski84.parquet4s

import org.openjdk.jmh.annotations._

import java.io.IOException
import java.nio.file.attribute.BasicFileAttributes
import java.nio.file._
import java.util.UUID
import java.util.concurrent.TimeUnit
import scala.collection.immutable
import scala.util.Random

case class Embedded(fraction: Double, text: String)
case class Record(i: Int, dict: String, embedded: Option[Embedded])


object CoreBenchmark {

  val Fractioner = 100.12
  val Dict = List("a", "b", "c", "d")

  @State(Scope.Benchmark)
  class Dataset {

    // 1024 & 512 * 1024
    @Param(Array("1024", "524288"))
    var datasetSize: Int = _
    var basePath: String = _
    var records: immutable.Iterable[Record] = _

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
    }

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

    def fetchDataset(dataset: Dataset): Unit = {
      this.dataset = dataset
      this.filePath = dataset.basePath + "/file.parquet"
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
      ParquetWriter.writeAndClose(filePath, dataset.records)

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
      ParquetReader.read[Record](dataset.basePath).last
  }

}

@Warmup(iterations = 5, time = 500, timeUnit = TimeUnit.MILLISECONDS)
@Measurement(iterations = 12, time = 500, timeUnit = TimeUnit.MILLISECONDS)
class CoreBenchmark {

  import CoreBenchmark._

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
