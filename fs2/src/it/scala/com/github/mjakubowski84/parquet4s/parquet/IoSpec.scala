package com.github.mjakubowski84.parquet4s.parquet

import cats.effect.IO
import cats.effect.testing.scalatest.AsyncIOSpec
import com.github.mjakubowski84.parquet4s.{Col, ParquetWriter, PartitionTestUtils, PartitionedPath, Path}
import fs2.Stream
import fs2.io.file.*
import org.apache.hadoop.io.SecureIOUtils.AlreadyExistsException
import org.apache.parquet.hadoop.ParquetFileWriter
import org.scalatest.Inside
import org.scalatest.flatspec.AsyncFlatSpec
import org.scalatest.matchers.should.Matchers

import scala.language.implicitConversions

class IoSpec extends AsyncFlatSpec with AsyncIOSpec with Matchers with PartitionTestUtils with Inside {

  implicit def parquetPathToFs2Path(path: Path): fs2.io.file.Path = fs2.io.file.Path.fromNioPath(path.toNio)

  implicit class Fs2PathWrapper(fs2Path: fs2.io.file.Path) {
    def toPath: Path = Path(fs2Path.toNioPath)
  }

  private val writeOptions = ParquetWriter.Options()

  private def createTempFileAtPath(path: Path): Stream[IO, Path] =
    Stream.eval(Files[IO].createDirectories(path)) >> Stream
      .resource(
        Files[IO]
          .tempFile(dir = Option(parquetPathToFs2Path(path)), suffix = ".parquet", prefix = "", permissions = None)
      )
      .as(path)

  "validateWritePath" should "fail if path already exists in create mode" in {
    val options = writeOptions.copy(writeMode = ParquetFileWriter.Mode.CREATE)

    val dirResource = Files[IO].tempDirectory(None, "", None).map(_.toPath)

    val testIO = dirResource.use { existingDir =>
      for {
        logger <- logger[IO](getClass)
        _      <- io.validateWritePath[IO](existingDir, options, logger)
      } yield succeed
    }

    testIO.assertThrows[AlreadyExistsException]
  }

  it should "delete existing path in overwrite mode" in {
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
      path <- Stream.resource(Files[IO].tempDirectory(None, "", None).map(_.toPath))
      dir  <- io.findPartitionedPaths[IO](path, writeOptions.hadoopConf)
    } yield {
      dir.schema should be(empty)
      dir.paths should be(empty)
    }

    testStream.compile.lastOrError
  }

  it should "return proper PartitionedDirectory for unpartitioned path with parquet content" in {
    val testStream = for {
      basePath <- Stream.resource(Files[IO].tempDirectory(None, "", None).map(_.toPath))
      _ <- Stream.resource(
        Files[IO]
          .tempFile(dir = Option(parquetPathToFs2Path(basePath)), suffix = ".parquet", prefix = "", permissions = None)
      )
      dir <- io.findPartitionedPaths[IO](basePath, writeOptions.hadoopConf)
    } yield {
      dir.schema should be(empty)
      dir.paths should be(Vector(PartitionedPath(basePath, List.empty)))
    }

    testStream.compile.lastOrError
  }

  it should "return proper PartitionedDirectory for single partition" in {
    val testStream = for {
      basePath      <- Stream.resource(Files[IO].tempDirectory(None, "", None).map(_.toPath))
      partitionPath <- createTempFileAtPath(basePath.append("x=1"))
      dir           <- io.findPartitionedPaths[IO](basePath, writeOptions.hadoopConf)
    } yield {
      dir.schema should be(List(Col("x")))
      dir.paths should be(Vector(PartitionedPath(partitionPath, List(Col("x") -> "1"))))
    }

    testStream.compile.lastOrError
  }

  it should "return proper PartitionedDirectory for complex partition" in {
    val testStream = for {
      basePath   <- Stream.resource(Files[IO].tempDirectory(None, "", None).map(_.toPath))
      partition1 <- createTempFileAtPath(basePath.append("x=1/y=a/z=0_9"))
      partition2 <- createTempFileAtPath(basePath.append("x=1/y=b/z=1_0"))
      partition3 <- createTempFileAtPath(basePath.append("x=1/y=c/z=1_1"))
      partition4 <- createTempFileAtPath(basePath.append("x=2/y=b/z=1_2"))
      dir        <- io.findPartitionedPaths[IO](basePath, writeOptions.hadoopConf)
    } yield {
      dir.schema should be(List(Col("x"), Col("y"), Col("z")))
      dir.paths should contain theSameElementsAs Vector(
        PartitionedPath(partition1, List(Col("x") -> "1", Col("y") -> "a", Col("z") -> "0_9")),
        PartitionedPath(partition2, List(Col("x") -> "1", Col("y") -> "b", Col("z") -> "1_0")),
        PartitionedPath(partition3, List(Col("x") -> "1", Col("y") -> "c", Col("z") -> "1_1")),
        PartitionedPath(partition4, List(Col("x") -> "2", Col("y") -> "b", Col("z") -> "1_2"))
      )
    }

    testStream.compile.lastOrError
  }

  it should "fail in case of inconsistent directory [case 1]" in {
    val testStream = for {
      basePath <- Stream.resource(Files[IO].tempDirectory(None, "", None).map(_.toPath))
      _        <- createTempFileAtPath(basePath.append("x=1/y=a"))
      _        <- createTempFileAtPath(basePath.append("y=b/x=2"))
      _        <- io.findPartitionedPaths[IO](basePath, writeOptions.hadoopConf)
    } yield succeed

    testStream.compile.lastOrError.assertThrows[IllegalArgumentException]
  }

  it should "fail in case of inconsistent directory [case 2]" in {
    val testStream = for {
      basePath <- Stream.resource(Files[IO].tempDirectory(None, "", None).map(_.toPath))
      _        <- createTempFileAtPath(basePath.append("x=1/y=a"))
      _        <- createTempFileAtPath(basePath.append("x=1/y=a/z=0_9"))
      _        <- io.findPartitionedPaths[IO](basePath, writeOptions.hadoopConf)
    } yield succeed

    testStream.compile.lastOrError.assertThrows[IllegalArgumentException]
  }

  "PartitionRegexp" should "match valid partition names and values" in {
    forAll(ValidPartitionsTable) { case (name, value) =>
      inside(s"$name=$value") { case io.PartitionRegexp(`name`, `value`) =>
        succeed
      }
    }
  }

  it should "not match invalid partition names and values" in {
    forAll(InvalidPartitionsTable) { case (name, value) =>
      s"$name=$value" match {
        case io.PartitionRegexp(capturedName, capturedValue) =>
          fail(
            s"Expected no match for name [$name] and value [$value] " +
              s"but one was found: [$capturedName, $capturedValue]"
          )

        case _ =>
          succeed
      }
    }
  }
}
