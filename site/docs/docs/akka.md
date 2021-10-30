---
layout: docs
title: Integration with Akka Streams
permalink: docs/akka/
---

# Integration with Akka Streams

Parquet4s has an integration module that allows you to read and write Parquet files using Akka Streams. Just import:

```scala
"com.github.mjakubowski84" %% "parquet4s-akka" % "@VERSION@"
"org.apache.hadoop" % "hadoop-client" % yourHadoopVersion
```

`ParquetStreams` has a single `Source` for reading single file or a directory (can be [partitioned]({% link docs/partitioning.md %})), a `Sink`s for writing a single file and a sophisticated `Flow` for performing complex writes.

```scala mdoc:compile-only
import akka.NotUsed
import akka.actor.ActorSystem
import akka.stream.scaladsl.Source
import com.github.mjakubowski84.parquet4s.{ParquetReader, ParquetStreams, ParquetWriter, Path}
import org.apache.parquet.hadoop.ParquetFileWriter.Mode
import org.apache.parquet.hadoop.metadata.CompressionCodecName
import org.apache.hadoop.conf.Configuration

import scala.concurrent.duration._

case class User(userId: String, name: String, created: java.sql.Timestamp)

implicit val system: ActorSystem = ActorSystem()

val users: Source[User, NotUsed] = ???

val conf: Configuration = ??? // Set Hadoop configuration programmatically

// Please check all the available configuration options!
val writeOptions = ParquetWriter.Options(
  writeMode = Mode.OVERWRITE,
  compressionCodecName = CompressionCodecName.SNAPPY,
  hadoopConf = conf // optional hadoopConf
)

// Writes a single file.
users.runWith(
  ParquetStreams
    .toParquetSingleFile
    .of[User]
    .options(writeOptions)
    .write(Path("file:///data/users/user-303.parquet"))
)

// Tailored for writing indefinite streams.
// Writes file when chunk reaches size limit and when defined time period elapses.
// Can also partition files!
// Check all the parameters and example usage in project sources.
users.via(
  ParquetStreams
    .viaParquet
    .of[User]
    .maxCount(writeOptions.rowGroupSize)
    .maxDuration(30.seconds)
    .options(writeOptions)
    .write(Path("file:///data/users"))
).runForeach(user => println(s"Just wrote user ${user.userId}..."))
  
// Reads a file, files from the directory or a partitioned directory. 
// Please also have a look at the rest of parameters.
ParquetStreams
  .fromParquet
  .as[User]
  .options(ParquetReader.Options(hadoopConf = conf))
  .read(Path("file:///data/users"))
  .runForeach(println)
```

Please check [examples](https://github.com/mjakubowski84/parquet4s/tree/master/examples/src/main/scala/com/github/mjakubowski84/parquet4s/fs2) to learn more.
