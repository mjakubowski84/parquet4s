package com.github.mjakubowski84.parquet4s

import akka.actor.ActorSystem
import akka.stream.scaladsl.{Sink, Source}
import cats.effect.{Blocker, ContextShift, IO}
import com.fasterxml.jackson.core.`type`.TypeReference
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import fs2.Stream
import org.scalameter.api._
import org.scalameter.picklers.{Pickler, StringPickler}

import java.nio.file.{Files, Paths}
import java.util.UUID
import java.util.concurrent.{ExecutorService, Executors}
import scala.collection.immutable
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, ExecutionContext}
import scala.util.Random

object Benchmark extends Bench.OfflineReport {

  case class Embedded(fraction: Double, text: String)
  case class Record(i: Int, dict: String, embedded: Option[Embedded])
  case class Dataset(path: String, records: immutable.Iterable[Record]) {
    override def toString: String = s"Dataset[path=$path,size=${records.size}]"
  }
  private val mapper = new ObjectMapper()
  mapper.registerModule(DefaultScalaModule)
  private val ref = new TypeReference[Dataset] {}
  private val writer = mapper.writerFor(ref)
  private val reader = mapper.readerFor(ref)

  implicit object DatasetPickler extends Pickler[Dataset] {
    override def pickle(x: Dataset): Array[Byte] =
      StringPickler.pickle(writer.writeValueAsString(x))

    override def unpickle(a: Array[Byte], from: Int): (Dataset, Int) =
      (reader.readValue(StringPickler.unpickle(a)), from)
  }

  private val fractioner = 100.12
  private val datasetSize = 1024
  private val dict = List("a", "b", "c", "d")

  private val rootPath = Files.createTempDirectory("benchmark")

  private val records = (1 to datasetSize).map { i =>
    Record(
      i = i,
      dict = dict(Random.nextInt(dict.size - 1)),
      embedded = if (i % 2 == 0) Some(Embedded(1.toDouble / fractioner, UUID.randomUUID().toString))
      else None
    )
  }

  private val datasets = (
    for {
      path <- Gen.range("subPath")(1, 5, 1)
        .map(_.toString).map(rootPath.resolve).map(_.toString)
      dataset <- Gen.single("dataset")(Dataset(path, records))
    } yield dataset
  ).cached


  performance of "core" in {
    measure method "write" in {
      using(datasets) setUp {
        case Dataset(path, _) => Files.deleteIfExists(Paths.get(path))
      } in {
        case Dataset(path, records) => ParquetWriter.writeAndClose(path, records)
      }
    }
    measure method "read" in {
      using(datasets) setUp {
        case Dataset(path, records) => ParquetWriter.writeAndClose(path, records)
      } tearDown {
        case Dataset(path, _) => Files.deleteIfExists(Paths.get(path))
      } in {
        case Dataset(path, _) => ParquetReader.read[RowParquetRecord](path).toVector
      }
    }
  }

  private def akkaWrite(path: String, records: immutable.Iterable[Record])(implicit as: ActorSystem) =
    Await.ready(Source(records).runWith(ParquetStreams.toParquetSingleFile(path)), Duration.Inf)
  private def akkaRead(path: String)(implicit as: ActorSystem) =
    Await.ready(ParquetStreams.fromParquet[RowParquetRecord].read(path).runWith(Sink.ignore), Duration.Inf)

  performance of "akka" in {
    measure method "write" in {
      implicit var actorSystem: ActorSystem = null
      using(datasets) beforeTests {
        actorSystem = ActorSystem()
      } setUp {
        case Dataset(path, _) => Files.deleteIfExists(Paths.get(path))
      } afterTests {
        Await.result(actorSystem.terminate(), Duration.Inf)
      } in {
        case Dataset(path, records) => akkaWrite(path, records)
      }
    }
    measure method "read" in {
      implicit var actorSystem: ActorSystem = null
      using(datasets) beforeTests {
        actorSystem = ActorSystem()
      } setUp {
        case Dataset(path, records) => ParquetWriter.writeAndClose(path, records)
      } tearDown {
        case Dataset(path, _) => Files.deleteIfExists(Paths.get(path))
      } afterTests {
        Await.result(actorSystem.terminate(), Duration.Inf)
      } in {
        case Dataset(path, _) => akkaRead(path)
      }
    }
  }

  object Fs2Ctx {
    def apply(): Fs2Ctx = {
      val (blocker, closeBlocker) = Blocker[IO].allocated.unsafeRunSync()
      val threadPool = Executors.newFixedThreadPool(Runtime.getRuntime.availableProcessors())
      Fs2Ctx(
        threadPool = threadPool,
        contextShift = IO.contextShift(ExecutionContext.fromExecutor(threadPool)),
        blocker = blocker,
        closeBlocker = closeBlocker
      )
    }
  }
  case class Fs2Ctx(
    threadPool: ExecutorService,
    implicit val contextShift: ContextShift[IO],
    blocker: Blocker,
    closeBlocker: IO[Unit]
  ) {
    def close(): Unit = {
      closeBlocker.unsafeRunSync()
      threadPool.shutdown()
    }
  }

  private def fs2Write(path: String, records: immutable.Iterable[Record])(implicit ctx: Fs2Ctx): IO[Unit] = {
    import ctx._
    Stream
      .iterable(records)
      .through(parquet.writeSingleFile[IO, Record](blocker, path))
      .compile
      .drain
  }

  private def fs2Read(path: String)(implicit ctx: Fs2Ctx): IO[Unit] = {
    import ctx._
    parquet.fromParquet[IO, Record].read(blocker, path).compile.drain
  }

  performance of "fs2" in {
    measure method "write" in {
      implicit var ctx: Fs2Ctx = null
      var operation: IO[Unit] = null
      using(datasets) beforeTests {
        ctx = Fs2Ctx()
      } setUp {
        case Dataset(path, records) =>
          Files.deleteIfExists(Paths.get(path))
          operation = fs2Write(path, records)
      } afterTests {
        ctx.close()
      } in {
        _ => operation.unsafeRunSync()
      }
    }
    measure method "read" in {
      implicit var ctx: Fs2Ctx = null
      var operation: IO[Unit] = null
      using(datasets) beforeTests {
        ctx = Fs2Ctx()
      } setUp {
        case Dataset(path, records) =>
          ParquetWriter.writeAndClose(path, records)
          operation = fs2Read(path)
      } tearDown {
        case Dataset(path, _) => Files.deleteIfExists(Paths.get(path))
      } afterTests {
        ctx.close()
      } in {
        _ => operation.unsafeRunSync()
      }
    }
  }
}
