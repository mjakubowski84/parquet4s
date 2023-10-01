---
layout: docs
title: Read and write Parquet from and to Protobuf
permalink: docs/protobuf/
---

# Read and write Parquet from and to Protobuf

Using the original Java Parquet library, you can read and write parquet to and from Protbuf. Parquet4s has `custom` functions in its API, which could be leveraged for that. However, Protobuf Parquet can only be used with Java models, not to mention other issues that make it hard to use, especially in Scala. You would prefer to use [ScalaPB](https://scalapb.github.io/) in Scala projects, right? Thanks to Parquet4S, you can! Import ScalaPB extension to any Parquet4S project, either it is Akka / Pekko, FS2 or plain Scala:

```scala
"com.github.mjakubowski84" %% "parquet4s-scalapb" % "@VERSION@"
```

Follow the ScalaPB [documentation](https://scalapb.github.io/docs/installation) to generate your Scala model from `.proto` files.

Then, import Parquet4S type classes tailored for Protobuf. The rest of the code stays the same as in regular Parquet4S - no matter if that is Akka / Pekko, FS2 or core!

```scala mdoc:compile-only
import com.github.mjakubowski84.parquet4s.ScalaPBImplicits._
import com.github.mjakubowski84.parquet4s.{ParquetReader, ParquetWriter, Path}

import scala.util.Using

case class GeneratedProtobufData()

val data: Iterable[GeneratedProtobufData] = ??? // your data
val path: Path = ??? // path to write to / to read from

// write
ParquetWriter.of[GeneratedProtobufData].writeAndClose(path.append("data.parquet"), data)

// read
Using.resource(ParquetReader.as[GeneratedProtobufData].read(path))(_.foreach(println))
```

Please follow the [examples](https://github.com/mjakubowski84/parquet4s/tree/master/examples/src/main/scala/com/github/mjakubowski84/parquet4s/scalapb) to learn more.
