package com.github.mjakubowski84.parquet4s.parquet

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import cats.implicits._
import com.github.mjakubowski84.parquet4s.{ParquetWriter, PartitionedPath}
import fs2.Stream
import fs2.io.file._
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.SecureIOUtils.AlreadyExistsException
import org.apache.parquet.hadoop.ParquetFileWriter
import org.scalatest.flatspec.AsyncFlatSpec
import org.scalatest.matchers.should.Matchers

import java.nio.file
import java.nio.file.Paths
import scala.language.implicitConversions

class IoSpec extends AsyncFlatSpec with Matchers {

  private val writeOptions = ParquetWriter.Options()

  implicit def nioToHadoopPath(nio: java.nio.file.Path): Path = new Path("file", null, nio.toString)
  implicit def hadoopToNioPath(hadoop: Path): java.nio.file.Path = Paths.get(hadoop.toUri)

  private def createTempFileAtPath(path: Path): Stream[IO, file.Path] =
    Stream.eval(Files[IO].createDirectories(path))
      .flatMap(dirPath => Stream.resource(Files[IO].tempFile(dir = Option(dirPath), suffix = ".parquet")).as(dirPath))

  "validateWritePath" should "fail if path already exists in create mode" in {
    val options = writeOptions.copy(writeMode = ParquetFileWriter.Mode.CREATE)

    val dirResource = Files[IO].tempDirectory().map(dir => new Path(dir.toUri))

    val testIO = dirResource.use { existingDir =>
      for {
        logger <- logger[IO](getClass)
        _ <- io.validateWritePath[IO](existingDir, options, logger)
      } yield succeed
    }

    recoverToSucceededIf[AlreadyExistsException](testIO.unsafeToFuture())
  }

  it should "delete existing path in overwrite mode" in {
    val options = writeOptions.copy(writeMode = ParquetFileWriter.Mode.OVERWRITE)

    val dirResource = Files[IO].tempDirectory().map(dir => new Path(dir.toUri))

    val testIO = dirResource.use { existingDir =>
      for {
        logger <- logger[IO](getClass)
        _ <- io.validateWritePath[IO](existingDir, options, logger)
        pathStillExists <- Files[IO].exists(existingDir)
      } yield pathStillExists should be(false)
    }

    testIO.unsafeToFuture()
  }

  it should "pass if path does not exist in any mode" in {
    val createMode = writeOptions.copy(writeMode = ParquetFileWriter.Mode.CREATE)
    val overwriteMode = writeOptions.copy(writeMode = ParquetFileWriter.Mode.OVERWRITE)

    val dirResource = Files[IO].tempDirectory().map(dir => new Path(dir.toUri))

    val testIO = dirResource.use { dir =>
      for {
        logger <- logger[IO](getClass)
        _ <- io.validateWritePath[IO](dir.suffix("/x"), createMode, logger)
        _ <- io.validateWritePath[IO](dir.suffix("/y"), overwriteMode, logger)
      } yield succeed
    }

    testIO.unsafeToFuture()
  }

  "findPartitionedPaths" should "return empty PartitionedDirectory for empty path" in {
    val testStream = for {
      path <- Stream.resource(Files[IO].tempDirectory())
      dir <- io.findPartitionedPaths[IO](path, writeOptions.hadoopConf)
    } yield {
      dir.schema should be(empty)
      dir.paths should be(empty)
    }

    testStream.compile.lastOrError.unsafeToFuture()
  }

  it should "return proper PartitionedDirectory for unpartitioned path with parquet content" in {
    val testStream = for {
      basePath <- Stream.resource(Files[IO].tempDirectory())
      _ <- Stream.resource(Files[IO].tempFile(dir = Option(basePath), suffix = ".parquet"))
      dir <- io.findPartitionedPaths[IO](basePath, writeOptions.hadoopConf)
    } yield {
      dir.schema should be(empty)
      dir.paths should be(Vector(PartitionedPath(basePath, List.empty)))
    }

    testStream.compile.lastOrError.unsafeToFuture()
  }

  it should "return proper PartitionedDirectory for single partition" in {
    val testStream = for {
      basePath <- Stream.resource(Files[IO].tempDirectory())
      partitionPath <- createTempFileAtPath(basePath.resolve("x=1"))
      dir <- io.findPartitionedPaths[IO](basePath, writeOptions.hadoopConf)
    } yield {
      dir.schema should be(List("x"))
      dir.paths should be(Vector(PartitionedPath(partitionPath, List("x" -> "1"))))
    }

    testStream.compile.lastOrError.unsafeToFuture()
  }

  it should "return proper PartitionedDirectory for complex partition" in {
    val testStream = for {
      basePath <- Stream.resource(Files[IO].tempDirectory())
      partition1 <- createTempFileAtPath(basePath.resolve("x=1/y=a/z=0_9"))
      partition2 <- createTempFileAtPath(basePath.resolve("x=1/y=b/z=1_0"))
      partition3 <- createTempFileAtPath(basePath.resolve("x=1/y=c/z=1_1"))
      partition4 <- createTempFileAtPath(basePath.resolve("x=2/y=b/z=1_2"))
      dir <- io.findPartitionedPaths[IO](basePath, writeOptions.hadoopConf)
    } yield {
      dir.schema should be(List("x", "y", "z"))
      dir.paths should contain theSameElementsAs Vector(
        PartitionedPath(partition1, List("x" -> "1", "y" -> "a", "z" -> "0_9")),
        PartitionedPath(partition2, List("x" -> "1", "y" -> "b", "z" -> "1_0")),
        PartitionedPath(partition3, List("x" -> "1", "y" -> "c", "z" -> "1_1")),
        PartitionedPath(partition4, List("x" -> "2", "y" -> "b", "z" -> "1_2"))
      )
    }

    testStream.compile.lastOrError.unsafeToFuture()
  }

  it should "fail in case of inconsistent directory [case 1]" in {
    val testStream = for {
      basePath <- Stream.resource(Files[IO].tempDirectory())
      _ <- createTempFileAtPath(basePath.resolve("x=1/y=a"))
      _ <- createTempFileAtPath(basePath.resolve("y=b/x=2"))
      _ <- io.findPartitionedPaths[IO](basePath, writeOptions.hadoopConf)
    } yield succeed

    recoverToSucceededIf[IllegalArgumentException](testStream.compile.lastOrError.unsafeToFuture())
  }

  it should "fail in case of inconsistent directory [case 2]" in {
    val testStream = for {
      basePath <- Stream.resource(Files[IO].tempDirectory())
      _ <- createTempFileAtPath(basePath.resolve("x=1/y=a"))
      _ <- createTempFileAtPath(basePath.resolve("x=1/y=a/z=0_9"))
      _ <- io.findPartitionedPaths[IO](basePath, writeOptions.hadoopConf)
    } yield succeed

    recoverToSucceededIf[IllegalArgumentException](testStream.compile.lastOrError.unsafeToFuture())
  }

}
