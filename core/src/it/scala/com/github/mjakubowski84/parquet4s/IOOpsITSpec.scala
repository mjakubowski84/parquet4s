package com.github.mjakubowski84.parquet4s

import org.apache.hadoop.fs.FileAlreadyExistsException
import org.apache.parquet.hadoop.ParquetFileWriter.Mode
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.{BeforeAndAfter, EitherValues}
import org.slf4j.{Logger, LoggerFactory}

class IOOpsITSpec extends AnyFlatSpec with Matchers with IOOps with TestUtils with BeforeAndAfter with EitherValues {

  override protected val logger: Logger = LoggerFactory.getLogger(this.getClass)
  val vcc: ValueCodecConfiguration      = ValueCodecConfiguration.Default

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

  "listPartitionedDirectory" should "return no paths and no partitions for empty directory" in {
    fileSystem.mkdirs(tempPath.toHadoop)

    val dir = listPartitionedDirectory(tempPath, configuration, Filter.noopFilter, vcc).value
    dir.paths should be(empty)
    dir.schema should be(empty)
  }

  it should "accept a filter for empty directory" in {
    fileSystem.mkdirs(tempPath.toHadoop)

    val dir = listPartitionedDirectory(tempPath, configuration, Col("x") === "1", vcc).value
    dir.paths should be(empty)
    dir.schema should be(empty)
  }

  it should "return no paths and no partitions for a directory with empty partition" in {
    val pathX1 = tempPath.append("x=1")
    fileSystem.mkdirs(pathX1.toHadoop)

    val dir = listPartitionedDirectory(tempPath, configuration, Filter.noopFilter, vcc).value
    dir.paths should be(empty)
    dir.schema should be(empty) // ???
  }

  it should "create single element partition in a path" in {
    val pathX1 = tempPath.append("x=1").append("file.parquet")
    fileSystem.createNewFile(pathX1.toHadoop)

    val dir = listPartitionedDirectory(tempPath, configuration, Filter.noopFilter, vcc).value
    dir.paths should be(
      List(
        PartitionedPath(
          path               = pathX1,
          configuration      = configuration,
          partitions         = (Col("x") -> "1") :: Nil,
          filterPredicateOpt = None
        )
      )
    )
    dir.schema should be(Col("x") :: Nil)
  }

  it should "return a partitioned directory with single file of no partitions values" in {
    val pathX1 = tempPath.append("x=1").append("file.parquet")
    fileSystem.createNewFile(pathX1.toHadoop)

    val dir = listPartitionedDirectory(pathX1, configuration, Filter.noopFilter, vcc).value
    dir.paths should be(
      List(PartitionedPath(path = pathX1, configuration = configuration, partitions = Nil, filterPredicateOpt = None))
    )
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

    val dir = listPartitionedDirectory(tempPath, configuration, Filter.noopFilter, vcc).value
    dir.paths.toList should contain theSameElementsInOrderAs List(
      PartitionedPath(
        path               = path1,
        configuration      = configuration,
        partitions         = List(Col("x") -> "1", Col("y") -> "a", Col("z") -> "1_1"),
        filterPredicateOpt = None
      ),
      PartitionedPath(
        path               = path2,
        configuration      = configuration,
        partitions         = List(Col("x") -> "1", Col("y") -> "b", Col("z") -> "1_2"),
        filterPredicateOpt = None
      ),
      PartitionedPath(
        path               = path3,
        configuration      = configuration,
        partitions         = List(Col("x") -> "1", Col("y") -> "c", Col("z") -> "1_3"),
        filterPredicateOpt = None
      ),
      PartitionedPath(
        path               = path4,
        configuration      = configuration,
        partitions         = List(Col("x") -> "2", Col("y") -> "b", Col("z") -> "0_9"),
        filterPredicateOpt = None
      )
    )
    dir.schema should be(List(Col("x"), Col("y"), Col("z")))
  }

  it should "fail to create partitions from inconsistent directory [case1]" in {
    val path1 = tempPath.append("x=1/y=a/file.parquet")
    val path2 = tempPath.append("y=1/x=b/file.parquet")
    fileSystem.createNewFile(path1.toHadoop)
    fileSystem.createNewFile(path2.toHadoop)

    listPartitionedDirectory(tempPath, configuration, Filter.noopFilter, vcc) should be a Symbol("Left")
  }

  it should "fail to create partitions from inconsistent directory [case2]" in {
    val path1 = tempPath.append("x=1/y=a/file_in_a_leaf_dir.parquet")
    val path2 = tempPath.append("x=1/file_in_a_node_dir.parquet")
    fileSystem.createNewFile(path1.toHadoop)
    fileSystem.createNewFile(path2.toHadoop)

    listPartitionedDirectory(tempPath, configuration, Filter.noopFilter, vcc) should be a Symbol("Left")
  }

  it should "filter partitioned directory and leave only parition paths matching filter predicate" in {
    val path1 = tempPath.append("x=1/y=a/z=0").append("file.parquet")
    val path2 = tempPath.append("x=1/y=b/z=0").append("file.parquet")
    val path3 = tempPath.append("x=1/y=c/z=1").append("file.parquet")
    val path4 = tempPath.append("x=2/y=b/z=1").append("file.parquet")
    fileSystem.createNewFile(path1.toHadoop)
    fileSystem.createNewFile(path2.toHadoop)
    fileSystem.createNewFile(path3.toHadoop)
    fileSystem.createNewFile(path4.toHadoop)

    val filter = Col("x") === "1" && Col("z") === "1"

    val dir = listPartitionedDirectory(tempPath, configuration, filter, vcc).value
    dir.paths.toList should be(
      List(
        PartitionedPath(
          path               = path3,
          configuration      = configuration,
          partitions         = List(Col("x") -> "1", Col("y") -> "c", Col("z") -> "1"),
          filterPredicateOpt = None // no predicate is left for filter the file
        )
      )
    )
    dir.schema should be(List(Col("x"), Col("y"), Col("z")))
  }

  it should "handle the situation when no parition path matches filter predicate" in {
    val path1 = tempPath.append("x=1/y=a/z=0").append("file.parquet")
    val path2 = tempPath.append("x=1/y=b/z=0").append("file.parquet")
    val path3 = tempPath.append("x=1/y=c/z=1").append("file.parquet")
    val path4 = tempPath.append("x=2/y=b/z=1").append("file.parquet")
    fileSystem.createNewFile(path1.toHadoop)
    fileSystem.createNewFile(path2.toHadoop)
    fileSystem.createNewFile(path3.toHadoop)
    fileSystem.createNewFile(path4.toHadoop)

    val filter = Col("x") === "2" && Col("z") === "0"

    val dir = listPartitionedDirectory(tempPath, configuration, filter, vcc).value
    dir.paths should be(empty)
    dir.schema should be(empty)
  }

  it should "accept a predicate which doesn't refer to parition fields" in {
    val path1 = tempPath.append("x=1/y=a/z=0").append("file.parquet")
    val path2 = tempPath.append("x=1/y=b/z=0").append("file.parquet")
    val path3 = tempPath.append("x=1/y=c/z=1").append("file.parquet")
    val path4 = tempPath.append("x=2/y=b/z=1").append("file.parquet")
    fileSystem.createNewFile(path1.toHadoop)
    fileSystem.createNewFile(path2.toHadoop)
    fileSystem.createNewFile(path3.toHadoop)
    fileSystem.createNewFile(path4.toHadoop)

    val filter          = Col("a") === "1" && Col("b") === "X"
    val filterPredicate = filter.toPredicate(vcc)

    val dir = listPartitionedDirectory(tempPath, configuration, filter, vcc).value
    dir.paths.toList should contain theSameElementsInOrderAs List(
      PartitionedPath(
        path               = path1,
        configuration      = configuration,
        partitions         = List(Col("x") -> "1", Col("y") -> "a", Col("z") -> "0"),
        filterPredicateOpt = Some(filterPredicate)
      ),
      PartitionedPath(
        path               = path2,
        configuration      = configuration,
        partitions         = List(Col("x") -> "1", Col("y") -> "b", Col("z") -> "0"),
        filterPredicateOpt = Some(filterPredicate)
      ),
      PartitionedPath(
        path               = path3,
        configuration      = configuration,
        partitions         = List(Col("x") -> "1", Col("y") -> "c", Col("z") -> "1"),
        filterPredicateOpt = Some(filterPredicate)
      ),
      PartitionedPath(
        path               = path4,
        configuration      = configuration,
        partitions         = List(Col("x") -> "2", Col("y") -> "b", Col("z") -> "1"),
        filterPredicateOpt = Some(filterPredicate)
      )
    )
    dir.schema should be(List(Col("x"), Col("y"), Col("z")))
  }

  it should "accept a predicate which partially refers to parition fields" in {
    val path1 = tempPath.append("x=1/y=a/z=0").append("file.parquet")
    val path2 = tempPath.append("x=1/y=b/z=0").append("file.parquet")
    val path3 = tempPath.append("x=1/y=c/z=1").append("file.parquet")
    val path4 = tempPath.append("x=2/y=b/z=1").append("file.parquet")
    fileSystem.createNewFile(path1.toHadoop)
    fileSystem.createNewFile(path2.toHadoop)
    fileSystem.createNewFile(path3.toHadoop)
    fileSystem.createNewFile(path4.toHadoop)

    val filter             = Col("x") === "1" && Col("b") === "X"
    val rewrittenPredicate = (Col("b") === "X").toPredicate(vcc)

    val dir = listPartitionedDirectory(tempPath, configuration, filter, vcc).value
    dir.paths.toList should contain theSameElementsInOrderAs List(
      PartitionedPath(
        path               = path1,
        configuration      = configuration,
        partitions         = List(Col("x") -> "1", Col("y") -> "a", Col("z") -> "0"),
        filterPredicateOpt = Some(rewrittenPredicate)
      ),
      PartitionedPath(
        path               = path2,
        configuration      = configuration,
        partitions         = List(Col("x") -> "1", Col("y") -> "b", Col("z") -> "0"),
        filterPredicateOpt = Some(rewrittenPredicate)
      ),
      PartitionedPath(
        path               = path3,
        configuration      = configuration,
        partitions         = List(Col("x") -> "1", Col("y") -> "c", Col("z") -> "1"),
        filterPredicateOpt = Some(rewrittenPredicate)
      )
    )
    dir.schema should be(List(Col("x"), Col("y"), Col("z")))
  }

  it should "accept a filter for non-partitioned directory" in {
    val path1 = tempPath.append("file1.parquet")
    val path2 = tempPath.append("file2.parquet")
    fileSystem.createNewFile(path1.toHadoop)
    fileSystem.createNewFile(path2.toHadoop)

    val filter          = Col("x") === "2" && Col("z") === "0"
    val filterPredicate = filter.toPredicate(vcc)

    val dir = listPartitionedDirectory(tempPath, configuration, filter, vcc).value
    dir.paths.toList should contain theSameElementsInOrderAs (
      Seq(
        PartitionedPath(
          path               = path1,
          configuration      = configuration,
          partitions         = List.empty,
          filterPredicateOpt = Some(filterPredicate)
        ),
        PartitionedPath(
          path               = path2,
          configuration      = configuration,
          partitions         = List.empty,
          filterPredicateOpt = Some(filterPredicate)
        )
      )
    )
    dir.schema should be(empty)
  }

}
