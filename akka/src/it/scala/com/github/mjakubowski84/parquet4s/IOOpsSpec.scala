package com.github.mjakubowski84.parquet4s

import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.SecureIOUtils.AlreadyExistsException
import org.apache.parquet.hadoop.ParquetFileWriter.Mode
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.{BeforeAndAfter, EitherValues}
import org.slf4j.{Logger, LoggerFactory}

class IOOpsSpec
  extends AnyFlatSpec
    with Matchers
    with IOOps
    with TestUtils
    with BeforeAndAfter
    with EitherValues {

  override protected val logger: Logger = LoggerFactory.getLogger(this.getClass)

  before {
    clearTemp()
  }

  "validateWritePath" should "raise an exception when writing to existing path in CREATE mode" in {
    fileSystem.mkdirs(tempPath)
    fileSystem.exists(tempPath) should be(true)

    an[AlreadyExistsException] should be thrownBy validateWritePath(
      path = tempPath,
      writeOptions = ParquetWriter.Options(writeMode = Mode.CREATE)
    )

    fileSystem.exists(tempPath) should be(true)
  }

  it should "pass if path doesn't exist in CREATE mode" in {
    fileSystem.exists(tempPath) should be(false)

    validateWritePath(
      path = tempPath,
      writeOptions = ParquetWriter.Options(writeMode = Mode.CREATE)
    )

    fileSystem.exists(tempPath) should be(false)
  }

  it should "delete existing path in OVERRIDE mode" in {
    fileSystem.mkdirs(tempPath)
    fileSystem.exists(tempPath) should be(true)

    validateWritePath(
      path = tempPath,
      writeOptions = ParquetWriter.Options(writeMode = Mode.OVERWRITE)
    )

    fileSystem.exists(tempPath) should be(false)
  }

  it should "pass when writing to non-existing path in OVERRIDE mode" in {
    fileSystem.exists(tempPath) should be(false)

    validateWritePath(
      path = tempPath,
      writeOptions = ParquetWriter.Options(writeMode = Mode.OVERWRITE)
    )

    fileSystem.exists(tempPath) should be(false)
  }

  "findPartitionedPaths" should "return single path without partitions for empty directory" in {
    fileSystem.mkdirs(tempPath)

    val dir = findPartitionedPaths(tempPath, configuration).right.value
    dir.paths should be(List(PartitionedPath(tempPath, List.empty)))
    dir.schema should be(empty)
  }

  it should "create single element partition in" in {
    val pathX1 = new Path(tempPath, "x=1")
    fileSystem.mkdirs(pathX1)

    val dir = findPartitionedPaths(tempPath, configuration).right.value
    dir.paths should be(List(PartitionedPath(pathX1, ("x" -> "1") :: Nil)))
    dir.schema should be("x" :: Nil)
  }

  it should "create proper partitions for complex tree" in {
    val path1 = tempPath.suffix("/x=1/y=a/z=1_1")
    val path2 = tempPath.suffix("/x=1/y=b/z=1_2")
    val path3 = tempPath.suffix("/x=1/y=c/z=1_3")
    val path4 = tempPath.suffix("/x=2/y=b/z=0_9")
    fileSystem.mkdirs(path1)
    fileSystem.mkdirs(path2)
    fileSystem.mkdirs(path3)
    fileSystem.mkdirs(path4)

    val dir = findPartitionedPaths(tempPath, configuration).right.value
    dir.paths should contain theSameElementsAs List(
      PartitionedPath(path1, List("x" -> "1", "y" -> "a", "z" -> "1_1")),
      PartitionedPath(path2, List("x" -> "1", "y" -> "b", "z" -> "1_2")),
      PartitionedPath(path3, List("x" -> "1", "y" -> "c", "z" -> "1_3")),
      PartitionedPath(path4, List("x" -> "2", "y" -> "b", "z" -> "0_9"))
    )
    dir.schema should be(List("x", "y", "z"))
  }

  it should "fail to create partitions from inconsistent directory" in {
    val path1 = tempPath.suffix("/x=1/y=a")
    val path2 = tempPath.suffix("/y=1/x=b")
    fileSystem.mkdirs(path1)
    fileSystem.mkdirs(path2)

    findPartitionedPaths(tempPath, configuration) should be a 'Left
  }

}
