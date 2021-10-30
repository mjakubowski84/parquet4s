---
layout: docs
title: Partitioning
permalink: docs/partitioning/
---

# Partitioning

Parquet4s supports both reading partitions and partitioning data during writing. This feature is available **only** in Akka and FS2 modules. 

Reading partitions is handled by default by `fromParquet` function. Before data is read Parquet4s scans the directory and resolves partition fields and values. After reading each record is enriched according to partition directory tree the file resides in.

Writing partitioned data is available in `viaParquet`. You can specify by which columns data shall be partitioned and Parquet4s will automatically create proper directory structure and it will remove the fields from the written records (so that there is no data redundancy).

**Take note**: partition field must be a String.

In Akka:
```scala mdoc:compile-only
import akka.NotUsed
import akka.actor.ActorSystem
import akka.stream.scaladsl.{Sink, Source}
import com.github.mjakubowski84.parquet4s.{Col, ParquetStreams, Path}

implicit val actorSystem: ActorSystem = ActorSystem()

case class PartitionDate(year: String, month: String, day: String)
case class User(id: Long, name: String, date: PartitionDate)

val users: Source[User, NotUsed] = ???
val path = Path("path/to/user/directory")

// writing partitioned data
users.via(
  ParquetStreams
    .viaParquet
    .of[User]
    .partitionBy(Col("date.year"), Col("date.month"), Col("date.day"))
    .write(path)
).runWith(Sink.foreach(user => println(s"Just wrote $user")))

// reading partitioned data
ParquetStreams
  .fromParquet
  .as[User]
  .read(path)
  .runWith(Sink.foreach(user => println(s"Just read $user")))
```

In FS2:

```scala mdoc:compile-only
import cats.effect.{IO, IOApp}
import com.github.mjakubowski84.parquet4s.parquet.{fromParquet, viaParquet}
import com.github.mjakubowski84.parquet4s.{Col, Path}
import fs2.Stream

object Example extends IOApp.Simple {

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

