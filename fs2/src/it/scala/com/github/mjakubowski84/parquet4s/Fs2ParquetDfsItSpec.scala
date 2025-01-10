package com.github.mjakubowski84.parquet4s

import cats.effect.IO
import cats.effect.std.UUIDGen
import cats.effect.testing.scalatest.AsyncIOSpec
import com.github.mjakubowski84.parquet4s.Fs2ParquetDfsItSpec.Data
import com.github.mjakubowski84.parquet4s.testkit.AsyncForAllMiniDfsCluster
import fs2.{Pipe, Stream}
import org.scalatest.flatspec.AsyncFlatSpec

import scala.collection.compat.immutable.LazyList

object Fs2ParquetDfsItSpec {
  case class Data(i: Long)
}

class Fs2ParquetDfsItSpec extends AsyncFlatSpec with AsyncIOSpec with AsyncForAllMiniDfsCluster {
  private lazy val readOptions  = ParquetReader.Options(hadoopConf = hadoopConfiguration)
  private lazy val writeOptions = ParquetWriter.Options(hadoopConf = hadoopConfiguration)

  override protected val deleteDfsDirOnShutdown: Boolean = true

  behavior of "parquet4s-fs2"

  it should "concurrently read and write files" in {
    val program = for {
      // Given
      inPath  <- UUIDGen.randomUUID[IO].map(id => Path(s"/$id.parquet"))
      outPath <- UUIDGen.randomUUID[IO].map(id => Path(s"/$id.parquet"))
      _       <- Stream.iterable(makeTestData()).through(writeData(inPath)).compile.drain
      // When
      _ <- readData(inPath).map(d => d.copy(i = d.i + 1)).through(writeData(outPath)).compile.drain
    } yield ()
    // Then
    program.assertNoException
  }

  // Helpers

  private def readData(path: Path): Stream[IO, Data] =
    parquet
      .fromParquet[IO]
      .as[Data]
      .options(readOptions)
      .read(path)

  private def writeData(path: Path): Pipe[IO, Data, Nothing] =
    parquet
      .writeSingleFile[IO]
      .of[Data]
      .options(writeOptions)
      .write(path)

  private def makeTestData(): LazyList[Data] =
    LazyList.range(start = 0L, end = 1024L, step = 1L).map(Data.apply)
}
