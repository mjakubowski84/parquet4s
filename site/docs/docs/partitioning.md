---
layout: docs
title: Partitioning
permalink: docs/partitioning/
---

# Partitioning

Parquet4s supports both reading partitions and partitioning data during writing. Writing partitioned data is available **only** in Akka and FS2 modules. Reading partitions is enabled by default in Akka and FS2 but must be enabled explicitly in core module.

#### Akka & FS2

Reading partitions is handled by default by `fromParquet` function. Before data is read Parquet4s scans the directory and resolves partition fields and values. After reading each record is enriched according to partition directory tree the file resides in.

Writing partitioned data is available in `viaParquet`. You can specify by which columns data shall be partitioned and Parquet4s will automatically create proper directory structure and it will remove the fields from the written records (so that there is no data redundancy).

#### Core

As core module is commonly used in low-level applications enabling partition reading can add redundant overhead. Therefore, when using core module you have to tell Parquet4s explicitely to read partitions.

#### **Take note!** 

 - Partition field must be a String. 
 - The field cannot be null, be an Option or belong to the collection.
 - When reading partitioned data make sure that partition directory names follow Hive format.
 - Parquet4s takes care of building proper schemas for partitioned data. However, when you use a custom type and a custom schema definition remember to not include the partition field in the schema — because it is supposed to be encoded as a directory name.

In Akka:
```scala mdoc:compile-only
import akka.NotUsed
import akka.actor.ActorSystem
import akka.stream.scaladsl.{Sink, Source}
import com.github.mjakubowski84.parquet4s.{Col, ParquetStreams, Path}

object AkkaExample extends App {

  implicit val actorSystem: ActorSystem = ActorSystem()
  import actorSystem.dispatcher

  case class PartitionDate(year: String, month: String, day: String)
  case class User(id: Long, name: String, date: PartitionDate)

  val users: Source[User, NotUsed] = ???
  val path = Path("path/to/user/directory")

  def writePartitionedData = users.via(
    ParquetStreams
      .viaParquet
      .of[User]
      .partitionBy(Col("date.year"), Col("date.month"), Col("date.day"))
      .write(path)
  ).runWith(Sink.foreach(user => println(s"Just wrote $user")))

  def readPartitionedData = ParquetStreams
    .fromParquet
    .as[User]
    .read(path)
    .runWith(Sink.foreach(user => println(s"Just read $user")))

  for {
    _ <- writePartitionedData
    _ <- readPartitionedData
    _ <- actorSystem.terminate()
  } yield ()

}
```

In FS2:

```scala mdoc:compile-only
import cats.effect.{IO, IOApp}
import com.github.mjakubowski84.parquet4s.parquet.{fromParquet, viaParquet}
import com.github.mjakubowski84.parquet4s.{Col, Path}
import fs2.Stream

object FS2Example extends IOApp.Simple {

  case class PartitionDate(year: String, month: String, day: String)
  case class User(id: Long, name: String, date: PartitionDate)

  val users: Stream[IO, User] = ???
  val path = Path("path/to/user/directory")
  
  val writePipe =
    viaParquet[IO]
      .of[User]
      .partitionBy(Col("date.year"), Col("date.month"), Col("date.day"))
      .write(path)
  
  val readStream =
    fromParquet[IO]
      .as[User]
      .read(path)
      .printlns

  def run: IO[Unit] =  
    users
      .through(writePipe)
      .append(readStream)
      .compile
      .drain
  
}
```

In core:

```scala mdoc:compile-only
import com.github.mjakubowski84.parquet4s.{Col, ParquetReader, ParquetStreams, Path}

object CoreExample extends App {

  case class PartitionDate(year: String, month: String, day: String)
  case class User(id: Long, name: String, date: PartitionDate)

  val path = Path("path/to/user/directory")

  val users = ParquetReader.as[User].partitioned.read(path)
  try users.foreach(println)
  finally users.close()

}
```
