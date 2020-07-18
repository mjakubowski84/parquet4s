package com.github.mjakubowski84.parquet4s

import cats.effect.IO
import com.github.mjakubowski84.parquet4s.Fs2ParquetItSpec.Data
import org.apache.parquet.hadoop.metadata.CompressionCodecName
import org.scalatest.BeforeAndAfter
import org.scalatest.flatspec.AsyncFlatSpec
import org.scalatest.matchers.should.Matchers

import scala.collection.compat.immutable.LazyList
import scala.util.Random
import fs2.Stream

object Fs2ParquetItSpec {

  case class Data(i: Long, s: String)

}

class Fs2ParquetItSpec extends AsyncFlatSpec with Matchers with TestUtils with BeforeAndAfter {

  before {
    clearTemp()
  }

  val writeOptions: ParquetWriter.Options = ParquetWriter.Options(
    compressionCodecName = CompressionCodecName.SNAPPY,
    pageSize = 512,
    rowGroupSize = 4 * 512,
    hadoopConf = configuration
  )

  val count: Int = 4 * writeOptions.rowGroupSize
  val dict: Seq[String] = Vector("a", "b", "c", "d")
  val data: LazyList[Data] = LazyList
    .range(start = 0L, end = count, step = 1L)
    .map(i => Data(i = i, s = dict(Random.nextInt(4))))

  it should "write and read single parquet file" in {
    val writeIO = Stream
      .iterable(data)
      .through(parquet.writeSingleFile[Data, IO](tempPathString, writeOptions))
      .compile
      .drain

    val readIO = parquet.read[Data, IO](tempPathString).compile.toVector

    val testIO = for {
      _ <- writeIO
      readData <- readIO
    } yield {
      readData.foreach(println)
      readData should contain theSameElementsInOrderAs data
    }

    testIO.unsafeToFuture()
  }


}
