package com.github.mjakubowski84.parquet4s

import com.github.mjakubowski84.parquet4s.ParquetDfsItSpec.Data
import com.github.mjakubowski84.parquet4s.testkit.ForAllMiniDfsCluster
import org.scalatest.flatspec.AnyFlatSpec

import java.util.UUID

object ParquetDfsItSpec {
  case class Data(i: Long)
}

class ParquetDfsItSpec extends AnyFlatSpec with ForAllMiniDfsCluster {
  private lazy val readOptions  = ParquetReader.Options(hadoopConf = hadoopConfiguration)
  private lazy val writeOptions = ParquetWriter.Options(hadoopConf = hadoopConfiguration)

  override protected val deleteDfsDirOnShutdown: Boolean = true

  behavior of "parquet4s-core"

  it should "concurrently read and write files" in {
    // Given
    val inPath  = Path(s"/${UUID.randomUUID()}.parquet")
    val outPath = Path(s"/${UUID.randomUUID()}.parquet")
    ParquetWriter.of[Data].options(writeOptions).writeAndClose(inPath, makeTestData())
    // When
    val outWriter  = ParquetWriter.of[Data].options(writeOptions).build(outPath)
    val inIterable = ParquetReader.as[Data].options(readOptions).read(inPath)

    try outWriter.write(inIterable.map(d => d.copy(i = d.i + 1)))
    finally {
      inIterable.close()
      outWriter.close()
    }
    // Then
    succeed
  }

  // Helpers

  private def makeTestData(): Iterable[Data] =
    (1L to 1024L).map(Data.apply)
}
