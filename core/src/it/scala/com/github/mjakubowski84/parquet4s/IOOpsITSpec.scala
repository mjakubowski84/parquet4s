package com.github.mjakubowski84.parquet4s

import org.apache.hadoop.fs.FileAlreadyExistsException
import org.apache.parquet.hadoop.ParquetFileWriter.Mode
import org.scalatest.BeforeAndAfter
import org.scalatest.EitherValues
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.slf4j.Logger
import org.slf4j.LoggerFactory

class IOOpsITSpec extends AnyFlatSpec with Matchers with IOOps with TestUtils with BeforeAndAfter with EitherValues {

  override protected val logger: Logger = LoggerFactory.getLogger(this.getClass)

  before {
    clearTemp()
  }

  "validateWritePath" should "succeed when writing to an existing directory path in CREATE mode" in {
    fileSystem.mkdirs(tempPath.toHadoop)
    fileSystem.exists(tempPath.toHadoop) should be(true)

    validateWritePath(
      path         = tempPath,
      writeOptions = ParquetWriter.Options(writeMode = Mode.CREATE)
    )

    fileSystem.exists(tempPath.toHadoop) should be(true)
  }

  it should "raise an exception when writing to an existing file path in CREATE mode" in {
    val filePath = tempPath.append("file.parquet")
    fileSystem.mkdirs(tempPath.toHadoop)
    fileSystem.createNewFile(filePath.hadoopPath)
    fileSystem.exists(filePath.toHadoop) should be(true)

    a[FileAlreadyExistsException] should be thrownBy validateWritePath(
      path         = filePath,
      writeOptions = ParquetWriter.Options(writeMode = Mode.CREATE)
    )

    fileSystem.exists(filePath.toHadoop) should be(true)
  }

  it should "pass if directory path doesn't exist in CREATE mode" in {
    fileSystem.exists(tempPath.toHadoop) should be(false)

    validateWritePath(
      path         = tempPath,
      writeOptions = ParquetWriter.Options(writeMode = Mode.CREATE)
    )

    fileSystem.exists(tempPath.toHadoop) should be(false)
  }

  it should "pass if file path doesn't exist in CREATE mode" in {
    val filePath = tempPath.append("file.parquet")
    fileSystem.exists(filePath.toHadoop) should be(false)

    validateWritePath(
      path         = filePath,
      writeOptions = ParquetWriter.Options(writeMode = Mode.CREATE)
    )

    fileSystem.exists(filePath.toHadoop) should be(false)
  }

  it should "delete existing directory path in OVERRIDE mode" in {
    fileSystem.mkdirs(tempPath.toHadoop)
    fileSystem.exists(tempPath.toHadoop) should be(true)

    validateWritePath(
      path         = tempPath,
      writeOptions = ParquetWriter.Options(writeMode = Mode.OVERWRITE)
    )

    fileSystem.exists(tempPath.toHadoop) should be(false)
  }

  it should "delete existing file path in OVERRIDE mode" in {
    val filePath = tempPath.append("file.parquet")
    fileSystem.mkdirs(tempPath.toHadoop)
    fileSystem.createNewFile(filePath.hadoopPath)
    fileSystem.exists(filePath.toHadoop) should be(true)

    validateWritePath(
      path         = filePath,
      writeOptions = ParquetWriter.Options(writeMode = Mode.OVERWRITE)
    )

    fileSystem.exists(filePath.toHadoop) should be(false)
  }

  it should "pass when writing to a non-existing directory path in OVERRIDE mode" in {
    fileSystem.exists(tempPath.toHadoop) should be(false)

    validateWritePath(
      path         = tempPath,
      writeOptions = ParquetWriter.Options(writeMode = Mode.OVERWRITE)
    )

    fileSystem.exists(tempPath.toHadoop) should be(false)
  }

  it should "pass when writing to a non-existing file path in OVERRIDE mode" in {
    val filePath = tempPath.append("file.parquet")
    fileSystem.exists(filePath.toHadoop) should be(false)

    validateWritePath(
      path         = filePath,
      writeOptions = ParquetWriter.Options(writeMode = Mode.OVERWRITE)
    )

    fileSystem.exists(filePath.toHadoop) should be(false)
  }

  "findPartitionedPaths" should "return no paths and no partitions for empty directory" in {
    fileSystem.mkdirs(tempPath.toHadoop)

    val dir = findPartitionedPaths(tempPath, configuration).value
    dir.paths should be(empty)
    dir.schema should be(empty)
  }

  it should "return no paths and no partitions for a directory with empty partition" in {
    val pathX1 = tempPath.append("x=1")
    fileSystem.mkdirs(pathX1.toHadoop)

    val dir = findPartitionedPaths(tempPath, configuration).value
    dir.paths should be(empty)
    dir.schema should be(empty) // ???
  }

  it should "create single element partition in a path" in {
    val pathX1 = tempPath.append("x=1").append("file.parquet")
    fileSystem.createNewFile(pathX1.toHadoop)

    val dir = findPartitionedPaths(tempPath, configuration).value
    dir.paths should be(List(PartitionedPath(pathX1, configuration, (Col("x") -> "1") :: Nil)))
    dir.schema should be(Col("x") :: Nil)
  }

  it should "ignore paths that do not match the pathFilter predicate" in {
    val path_X1 = tempPath.append("_x=1").append("file.parquet")
    val pathX1  = tempPath.append("x=1").append("file.parquet")
    fileSystem.createNewFile(path_X1.toHadoop)
    fileSystem.createNewFile(pathX1.toHadoop)

    val dir = findPartitionedPaths(tempPath, configuration, !_.getName().startsWith("_")).value
    dir.paths should be(List(PartitionedPath(pathX1, configuration, (Col("x") -> "1") :: Nil)))
    dir.schema should be(List(Col("x")))
  }

  it should "accept hadoop default hidden paths when pathFilter always returns true" in {
    val pathX1 = tempPath.append("_x=1/.y=1").append("file.parquet")
    fileSystem.createNewFile(pathX1.toHadoop)

    val dir = findPartitionedPaths(tempPath, configuration, _ => true).value
    dir.paths should be(List(PartitionedPath(pathX1, configuration, List(Col("_x") -> "1", Col(".y") -> "1"))))
    dir.schema should be(List(Col("_x"), Col(".y")))
  }

  it should "return a partitioned directory with single file of no partitions values" in {
    val pathX1 = tempPath.append("x=1").append("file.parquet")
    fileSystem.createNewFile(pathX1.toHadoop)

    val dir = findPartitionedPaths(pathX1, configuration).value
    dir.paths should be(List(PartitionedPath(pathX1, configuration, Nil)))
    dir.schema should be(empty)
  }

  it should "create proper partitions for complex tree" in {
    val path1 = tempPath.append("x=1/y=a/z=1_1").append("file.parquet")
    val path2 = tempPath.append("x=1/y=b/z=1_2").append("file.parquet")
    val path3 = tempPath.append("x=1/y=c/z=1_3").append("file.parquet")
    val path4 = tempPath.append("x=2/y=b/z=0_9").append("file.parquet")
    fileSystem.createNewFile(path1.toHadoop)
    fileSystem.createNewFile(path2.toHadoop)
    fileSystem.createNewFile(path3.toHadoop)
    fileSystem.createNewFile(path4.toHadoop)

    val dir = findPartitionedPaths(tempPath, configuration).value
    dir.paths should contain theSameElementsAs List(
      PartitionedPath(path1, configuration, List(Col("x") -> "1", Col("y") -> "a", Col("z") -> "1_1")),
      PartitionedPath(path2, configuration, List(Col("x") -> "1", Col("y") -> "b", Col("z") -> "1_2")),
      PartitionedPath(path3, configuration, List(Col("x") -> "1", Col("y") -> "c", Col("z") -> "1_3")),
      PartitionedPath(path4, configuration, List(Col("x") -> "2", Col("y") -> "b", Col("z") -> "0_9"))
    )
    dir.schema should be(List(Col("x"), Col("y"), Col("z")))
  }

  it should "fail to create partitions from inconsistent directory [case1]" in {
    val path1 = tempPath.append("x=1/y=a/file.parquet")
    val path2 = tempPath.append("y=1/x=b/file.parquet")
    fileSystem.createNewFile(path1.toHadoop)
    fileSystem.createNewFile(path2.toHadoop)

    findPartitionedPaths(tempPath, configuration) should be a Symbol("Left")
  }

  it should "fail to create partitions from inconsistent directory [case2]" in {
    val path1 = tempPath.append("x=1/y=a/file_in_a_leaf_dir.parquet")
    val path2 = tempPath.append("x=1/file_in_a_node_dir.parquet")
    fileSystem.createNewFile(path1.toHadoop)
    fileSystem.createNewFile(path2.toHadoop)

    findPartitionedPaths(tempPath, configuration) should be a Symbol("Left")
  }

  it should "fail to create partitions from inconsistent directory [case3]" in {
    val path1 = tempPath.append("x=1/file.parquet")
    val path2 = tempPath.append("_x=1/file.parquet")
    fileSystem.createNewFile(path1.toHadoop)
    fileSystem.createNewFile(path2.toHadoop)
    // This case ignores `_x=1`, so there's no inconsistent directory.
    findPartitionedPaths(tempPath, configuration) should be a Symbol("Right")
    // This case regards `_x=1` an as ordinary directory, so it conflicts with `x=1`.
    findPartitionedPaths(tempPath, configuration, _ => true) should be a Symbol("Left")
  }

}
