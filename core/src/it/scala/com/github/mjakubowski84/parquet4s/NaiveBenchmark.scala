package com.github.mjakubowski84.parquet4s

import java.io.IOException
import java.nio.file.attribute.BasicFileAttributes
import java.util.UUID
import scala.util.Random
import java.nio.file.{FileVisitResult, FileVisitor, Files, Paths, Path => NioPath}

object NaiveBenchmark {

  case class Embedded(fraction: Double, text: String)
  case class Record(i: Int, dict: String, embedded: Option[Embedded])

  private val fractioner = 100.12
  private val datasetSize = 1024 * 512
  private val dict = List("a", "b", "c", "d")
  private val warmUpRounds = 12
  private val rounds = 100

  private lazy val data = (1 to datasetSize).map { i =>
    Record(
      i = i,
      dict = dict(Random.nextInt(dict.size - 1)),
      embedded = if (i % 2 == 0) Some(Embedded(1.toDouble / fractioner, UUID.randomUUID().toString))
      else None
    )
  }

  private val rootPath: NioPath = Files.createTempDirectory("benchmark")

  private def deletePath(path: String) = Files.walkFileTree(Paths.get(path), new FileVisitor[NioPath]() {
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

  private def write(childPath: NioPath): Long = {
    val start = System.currentTimeMillis()
    ParquetWriter.writeAndClose(
      path = Path(rootPath.resolve(childPath)),
      data = data
    )
    val end = System.currentTimeMillis()
    end - start
  }

  private def read(childPath: NioPath): Long = {
    val start = System.currentTimeMillis()
    ParquetReader.read[Record](
      path = Path(rootPath.resolve(childPath))
    ).last
    val end = System.currentTimeMillis()
    end - start
  }

  private def warmUpRound(round: Int): Unit = {
    val warmUpPath = Paths.get(s"warm-up-$round")
    val time = write(warmUpPath) + read(warmUpPath)
    println(s"$round time: " + time)
  }

  private def warmUp(): Unit = {
    println("WarmUp")
    for (i <- 1 to warmUpRounds) warmUpRound(i)
    println("------")
  }


  private def test(round: Int): (Long, Long) = {
    print(".")
    val roundPath = Paths.get(round.toString)
    (write(roundPath), read(roundPath))
  }

  private def printTimes(times: Seq[Long], name: String): Unit = {
    val max = times.max
    val min = times.min
    val mean = times.sorted.apply(rounds / 2)
    println(s"[$name] min = $min, max = $max, mean = $mean")
  }

  def main(args: Array[String]): Unit = {
    warmUp()
    println("Test")
    val times = (1 to rounds).map(test)
    val writeTimes = times.map(_._1)
    val readTimes = times.map(_._2)
    println()
    println("Results:")
    printTimes(writeTimes, "write")
    printTimes(readTimes, "read")
  }

}
