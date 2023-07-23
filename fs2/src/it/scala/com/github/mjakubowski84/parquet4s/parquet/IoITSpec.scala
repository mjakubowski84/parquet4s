package com.github.mjakubowski84.parquet4s.parquet

import cats.effect.IO
import cats.effect.testing.scalatest.AsyncIOSpec
import com.github.mjakubowski84.parquet4s.{Col, ParquetWriter, PartitionedPath, Path}
import fs2.Stream
import fs2.io.file.*
import org.apache.parquet.hadoop.ParquetFileWriter
import org.scalatest.flatspec.AsyncFlatSpec
import org.scalatest.matchers.should.Matchers

import scala.language.implicitConversions

class IoITSpec extends AsyncFlatSpec with AsyncIOSpec with Matchers {

  implicit def parquetPathToFs2Path(path: Path): fs2.io.file.Path = fs2.io.file.Path.fromNioPath(path.toNio)

  implicit class Fs2PathWrapper(fs2Path: fs2.io.file.Path) {
    def toPath: Path = Path(fs2Path.toNioPath)
  }

  private val writeOptions  = ParquetWriter.Options()
  private val configuration = writeOptions.hadoopConf
  private val fileName      = "file.parquet"

  private def createFileAtPath(path: Path): Stream[IO, Path] = {
    val filePath = path.append(fileName)
    Stream.eval(Files[IO].createDirectories(path)) >> Stream
      .eval(Files[IO].createFile(filePath))
      .as(filePath)
  }

  "validateWritePath" should "succeed if path already exists in create mode" in {
    val options = writeOptions.copy(writeMode = ParquetFileWriter.Mode.CREATE)

    val dirResource = Files[IO].tempDirectory(None, "", None).map(_.toPath)

    dirResource.use { existingDir =>
      for {
        logger <- logger[IO](getClass)
        _      <- io.validateWritePath[IO](existingDir, options, logger)
      } yield succeed
    }
  }

  it should "fail if file path already exists in create mode" in {
    val options = writeOptions.copy(writeMode = ParquetFileWriter.Mode.CREATE)

    val fileResource = Files[IO]
      .tempDirectory(None, "", None)
      .flatMap(dirPath =>
        Files[IO].tempFile(dir = Option(dirPath), suffix = ".parquet", prefix = "", permissions = None)
      )
      .map(_.toPath)

    val testIO = fileResource.use { existingFile =>
      for {
        logger <- logger[IO](getClass)
        _      <- io.validateWritePath[IO](existingFile, options, logger)
      } yield succeed
    }

    testIO.assertThrows[org.apache.hadoop.fs.FileAlreadyExistsException]
  }

  it should "delete existing directory path in overwrite mode" in {
    val options = writeOptions.copy(writeMode = ParquetFileWriter.Mode.OVERWRITE)

    val fileResource = Files[IO]
      .tempDirectory(None, "", None)
      .flatMap(dirPath =>
        Files[IO].tempFile(dir = Option(dirPath), suffix = ".parquet", prefix = "", permissions = None)
      )
      .map(_.toPath)

    fileResource.use { existingFile =>
      for {
        logger          <- logger[IO](getClass)
        _               <- io.validateWritePath[IO](existingFile, options, logger)
        pathStillExists <- Files[IO].exists(existingFile)
      } yield pathStillExists should be(false)
    }
  }

  it should "delete existing file path in overwrite mode" in {
    val options = writeOptions.copy(writeMode = ParquetFileWriter.Mode.OVERWRITE)

    val dirResource = Files[IO].tempDirectory(None, "", None).map(_.toPath)

    dirResource.use { existingDir =>
      for {
        logger          <- logger[IO](getClass)
        _               <- io.validateWritePath[IO](existingDir, options, logger)
        pathStillExists <- Files[IO].exists(existingDir)
      } yield pathStillExists should be(false)
    }
  }

  it should "pass if path does not exist in any mode" in {
    val createMode    = writeOptions.copy(writeMode = ParquetFileWriter.Mode.CREATE)
    val overwriteMode = writeOptions.copy(writeMode = ParquetFileWriter.Mode.OVERWRITE)

    val dirResource = Files[IO].tempDirectory(None, "", None).map(_.toPath)

    dirResource.use { dir =>
      for {
        logger <- logger[IO](getClass)
        _      <- io.validateWritePath[IO](dir.append("x"), createMode, logger)
        _      <- io.validateWritePath[IO](dir.append("y"), overwriteMode, logger)
      } yield succeed
    }
  }

  "findPartitionedPaths" should "return empty PartitionedDirectory for empty path" in {
    val testStream = for {
      path   <- Stream.resource(Files[IO].tempDirectory(None, "", None).map(_.toPath))
      logger <- Stream.eval(logger[IO](getClass))
      dir    <- io.findPartitionedPaths[IO](path, writeOptions.hadoopConf, logger)
    } yield {
      dir.schema should be(empty)
      dir.paths should be(empty)
    }

    testStream.compile.lastOrError
  }

  it should "return proper PartitionedDirectory for unpartitioned path with parquet content" in {
    val testStream = for {
      basePath <- Stream.resource(Files[IO].tempDirectory(None, "", None).map(_.toPath))
      filePath <- createFileAtPath(basePath)
      logger   <- Stream.eval(logger[IO](getClass))
      dir      <- io.findPartitionedPaths[IO](basePath, writeOptions.hadoopConf, logger)
    } yield {
      dir.schema should be(empty)
      dir.paths should be(Vector(PartitionedPath(filePath, configuration, List.empty)))
    }

    testStream.compile.lastOrError
  }

  it should "return proper PartitionedDirectory for single partition" in {
    val testStream = for {
      basePath      <- Stream.resource(Files[IO].tempDirectory(None, "", None).map(_.toPath))
      partitionPath <- createFileAtPath(basePath.append("x=1"))
      logger        <- Stream.eval(logger[IO](getClass))
      dir           <- io.findPartitionedPaths[IO](basePath, writeOptions.hadoopConf, logger)
    } yield {
      dir.schema should be(List(Col("x")))
      dir.paths should be(Vector(PartitionedPath(partitionPath, configuration, List(Col("x") -> "1"))))
    }

    testStream.compile.lastOrError
  }

  it should "return proper PartitionedDirectory for a path pointing a file" in {
    val testStream = for {
      basePath      <- Stream.resource(Files[IO].tempDirectory(None, "", None).map(_.toPath))
      partitionPath <- createFileAtPath(basePath.append("x=1"))
      logger        <- Stream.eval(logger[IO](getClass))
      dir           <- io.findPartitionedPaths[IO](partitionPath, writeOptions.hadoopConf, logger)
    } yield {
      dir.schema should be(empty)
      dir.paths should be(Vector(PartitionedPath(partitionPath, configuration, List.empty)))
    }

    testStream.compile.lastOrError
  }

  it should "return proper PartitionedDirectory for complex partition" in {
    val testStream = for {
      basePath   <- Stream.resource(Files[IO].tempDirectory(None, "", None).map(_.toPath))
      partition1 <- createFileAtPath(basePath.append("x=1/y=a/z=0_9"))
      partition2 <- createFileAtPath(basePath.append("x=1/y=b/z=1_0"))
      partition3 <- createFileAtPath(basePath.append("x=1/y=c/z=1_1"))
      partition4 <- createFileAtPath(basePath.append("x=2/y=b/z=1_2"))
      logger     <- Stream.eval(logger[IO](getClass))
      dir        <- io.findPartitionedPaths[IO](basePath, writeOptions.hadoopConf, logger)
    } yield {
      dir.schema should be(List(Col("x"), Col("y"), Col("z")))
      dir.paths should contain theSameElementsAs Vector(
        PartitionedPath(partition1, configuration, List(Col("x") -> "1", Col("y") -> "a", Col("z") -> "0_9")),
        PartitionedPath(partition2, configuration, List(Col("x") -> "1", Col("y") -> "b", Col("z") -> "1_0")),
        PartitionedPath(partition3, configuration, List(Col("x") -> "1", Col("y") -> "c", Col("z") -> "1_1")),
        PartitionedPath(partition4, configuration, List(Col("x") -> "2", Col("y") -> "b", Col("z") -> "1_2"))
      )
    }

    testStream.compile.lastOrError
  }

  it should "fail in case of inconsistent directory [case 1]" in {
    val testStream = for {
      basePath <- Stream.resource(Files[IO].tempDirectory(None, "", None).map(_.toPath))
      _        <- createFileAtPath(basePath.append("x=1/y=a"))
      _        <- createFileAtPath(basePath.append("y=b/x=2"))
      logger   <- Stream.eval(logger[IO](getClass))
      _        <- io.findPartitionedPaths[IO](basePath, writeOptions.hadoopConf, logger)
    } yield succeed

    testStream.compile.lastOrError.assertThrows[IllegalArgumentException]
  }

  it should "fail in case of inconsistent directory [case 2]" in {
    val testStream = for {
      basePath <- Stream.resource(Files[IO].tempDirectory(None, "", None).map(_.toPath))
      _        <- createFileAtPath(basePath.append("x=1/y=a"))
      _        <- createFileAtPath(basePath.append("x=1/y=a/z=0_9"))
      logger   <- Stream.eval(logger[IO](getClass))
      _        <- io.findPartitionedPaths[IO](basePath, writeOptions.hadoopConf, logger)
    } yield succeed

    testStream.compile.lastOrError.assertThrows[IllegalArgumentException]
  }

}
