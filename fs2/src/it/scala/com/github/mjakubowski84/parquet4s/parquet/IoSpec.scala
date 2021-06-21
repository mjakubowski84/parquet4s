package com.github.mjakubowski84.parquet4s.parquet

import java.nio.file
import java.nio.file.Paths

import cats.effect.{Blocker, ContextShift, IO}
import com.github.mjakubowski84.parquet4s.{ParquetWriter, PartitionTestUtils, PartitionedPath}
import fs2.Stream
import fs2.io.file._
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.SecureIOUtils.AlreadyExistsException
import org.apache.parquet.hadoop.ParquetFileWriter
import org.scalatest.flatspec.AsyncFlatSpec
import org.scalatest.matchers.should.Matchers
import scala.language.implicitConversions

class IoSpec extends AsyncFlatSpec with Matchers with PartitionTestUtils {

  private implicit val contextShift: ContextShift[IO] = IO.contextShift(executionContext)
  private val writeOptions = ParquetWriter.Options()
  private val tmpDir = Paths.get(sys.props("java.io.tmpdir")).toAbsolutePath

  implicit def nioToHadoopPath(nio: java.nio.file.Path): Path = new Path("file", null, nio.toString)
  implicit def hadoopToNioPath(hadoop: Path): java.nio.file.Path = Paths.get(hadoop.toUri)

  private def createTempFileAtPath(blocker: Blocker, path: Path): Stream[IO, file.Path] =
    Stream.eval(createDirectories[IO](blocker, path))
      .flatMap(dirPath => tempFileStream[IO](blocker, dirPath, suffix = ".parquet").as(dirPath))

  "validateWritePath" should "fail if path already exists in create mode" in {
    val options = writeOptions.copy(writeMode = ParquetFileWriter.Mode.CREATE)

    val resources = for {
      blocker <- Blocker[IO]
      dir <- tempDirectoryResource[IO](blocker, tmpDir)
    } yield (blocker, new Path(dir.toUri))

    val testIO = resources.use { case (blocker, existingDir) =>
      for {
        logger <- logger[IO](getClass)
        _ <- io.validateWritePath[IO](blocker, existingDir, options, logger)
      } yield succeed
    }

    recoverToSucceededIf[AlreadyExistsException](testIO.unsafeToFuture())
  }

  it should "delete existing path in overwrite mode" in {
    val options = writeOptions.copy(writeMode = ParquetFileWriter.Mode.OVERWRITE)

    val resources = for {
      blocker <- Blocker[IO]
      dir <- tempDirectoryResource[IO](blocker, tmpDir)
    } yield (blocker, new Path(dir.toUri))

    val testIO = resources.use { case (blocker, existingDir) =>
      for {
        logger <- logger[IO](getClass)
        _ <- io.validateWritePath[IO](blocker, existingDir, options, logger)
        pathStillExists <- exists[IO](blocker, existingDir)
      } yield pathStillExists should be(false)
    }

    testIO.unsafeToFuture()
  }

  it should "pass if path does not exist in any mode" in {
    val createMode = writeOptions.copy(writeMode = ParquetFileWriter.Mode.CREATE)
    val overwriteMode = writeOptions.copy(writeMode = ParquetFileWriter.Mode.OVERWRITE)

    val testIO = Blocker[IO].use { blocker =>
      for {
        logger <- logger[IO](getClass)
        _ <- io.validateWritePath[IO](blocker, tmpDir.suffix("/x"), createMode, logger)
        _ <- io.validateWritePath[IO](blocker, tmpDir.suffix("/y"), overwriteMode, logger)
      } yield succeed
    }

    testIO.unsafeToFuture()
  }

  "findPartitionedPaths" should "return empty PartitionedDirectory for empty path" in {
    val testStream = for {
      blocker <- Stream.resource(Blocker[IO])
      path <- tempDirectoryStream[IO](blocker, tmpDir)
      dir <- io.findPartitionedPaths[IO](blocker, path, writeOptions.hadoopConf)
    } yield {
      dir.schema should be(empty)
      dir.paths should be(empty)
    }

    testStream.compile.lastOrError.unsafeToFuture()
  }

  it should "return proper PartitionedDirectory for unparitioned path with parquet content" in {
    val testStream = for {
      blocker <- Stream.resource(Blocker[IO])
      basePath <- tempDirectoryStream[IO](blocker, tmpDir)
      _ <- tempFileStream[IO](blocker, basePath, suffix = ".parquet")
      dir <- io.findPartitionedPaths[IO](blocker, basePath, writeOptions.hadoopConf)
    } yield {
      dir.schema should be(empty)
      dir.paths should be(Vector(PartitionedPath(basePath, List.empty)))
    }

    testStream.compile.lastOrError.unsafeToFuture()
  }

  it should "return proper PartitionedDirectory for single partition" in {
    val testStream = for {
      blocker <- Stream.resource(Blocker[IO])
      basePath <- tempDirectoryStream[IO](blocker, tmpDir)
      partitionPath <- createTempFileAtPath(blocker, basePath.resolve("x=1"))
      dir <- io.findPartitionedPaths[IO](blocker, basePath, writeOptions.hadoopConf)
    } yield {
      dir.schema should be(List("x"))
      dir.paths should be(Vector(PartitionedPath(partitionPath, List("x" -> "1"))))
    }

    testStream.compile.lastOrError.unsafeToFuture()
  }

  it should "return proper PartitionedDirectory for complex partition" in {
    val testStream = for {
      blocker <- Stream.resource(Blocker[IO])
      basePath <- tempDirectoryStream[IO](blocker, tmpDir)
      partition1 <- createTempFileAtPath(blocker, basePath.resolve("x=1/y=a/z=0_9"))
      partition2 <- createTempFileAtPath(blocker, basePath.resolve("x=1/y=b/z=1_0"))
      partition3 <- createTempFileAtPath(blocker, basePath.resolve("x=1/y=c/z=1_1"))
      partition4 <- createTempFileAtPath(blocker, basePath.resolve("x=2/y=b/z=1_2"))
      dir <- io.findPartitionedPaths[IO](blocker, basePath, writeOptions.hadoopConf)
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
      blocker <- Stream.resource(Blocker[IO])
      basePath <- tempDirectoryStream[IO](blocker, tmpDir)
      _ <- createTempFileAtPath(blocker, basePath.resolve("x=1/y=a"))
      _ <- createTempFileAtPath(blocker, basePath.resolve("y=b/x=2"))
      _ <- io.findPartitionedPaths[IO](blocker, basePath, writeOptions.hadoopConf)
    } yield succeed

    recoverToSucceededIf[IllegalArgumentException](testStream.compile.lastOrError.unsafeToFuture())
  }

  it should "fail in case of inconsistent directory [case 2]" in {
    val testStream = for {
      blocker <- Stream.resource(Blocker[IO])
      basePath <- tempDirectoryStream[IO](blocker, tmpDir)
      _ <- createTempFileAtPath(blocker, basePath.resolve("x=1/y=a"))
      _ <- createTempFileAtPath(blocker, basePath.resolve("x=1/y=a/z=0_9"))
      _ <- io.findPartitionedPaths[IO](blocker, basePath, writeOptions.hadoopConf)
    } yield succeed

    recoverToSucceededIf[IllegalArgumentException](testStream.compile.lastOrError.unsafeToFuture())
  }

  "PartitionRegexp" should "match valid partition names and values" in {
    val validNames = generatePartitionStrings(prefix = "testValue", withChars = allowedPartitionNameChars)
    val validValues = generatePartitionStrings(prefix = "testName", withChars = allowedPartitionValueChars)
    val validPairs = validNames.flatMap(name => validValues.map(value => name -> value))

    validPairs.foreach { case (name, value) =>
      s"$name=$value" match {
        case io.PartitionRegexp(`name`, `value`) =>
          succeed

        case _ =>
          fail(
            s"Expected a valid match for name [$name] and value [$value] but none was found"
          )
      }
    }

    succeed
  }

  it should "not match invalid partition names and values" in {
    val invalidNames = generatePartitionStrings(prefix = "testValue", withChars = disallowedPartitionNameChars)
    val invalidValues = generatePartitionStrings(prefix = "testName", withChars = disallowedPartitionValueChars)
    val invalidPairs = invalidNames.flatMap(name => invalidValues.map(value => name -> value))

    invalidPairs.foreach { case (name, value) =>
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

    succeed
  }
}
