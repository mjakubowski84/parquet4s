# Parquet4S

Simple I/O for [Parquet](https://parquet.apache.org/). Allows you to easily read and write Parquet files in [Scala](https://www.scala-lang.org/).

Use just a Scala case class to define the schema of your data. No need to use Avro, Protobuf, Thrift or other data serialisation systems. You can use generic records if you don't want to use the case class, too.

Compatible with files generated with [Apache Spark](https://spark.apache.org/). However, unlike in Spark, you do not have to start a cluster to perform I/O operations.

Based on official [Parquet library](https://github.com/apache/parquet-mr), [Hadoop Client](https://github.com/apache/hadoop) and [Shapeless](https://github.com/milessabin/shapeless).

Integrations for [Akka Streams](https://doc.akka.io/docs/akka/current/stream/index.html) and [FS2](https://fs2.io/).

Released for Scala 2.11.x, 2.12.x and 2.13.x. FS2 integration is available for 2.12.x and 2.13.x.

## Tutorial

1. [Quick Start](#quick-start)
1. [AWS S3](#aws-s3)
1. [Akka Streams](#akka-streams)
1. [FS2](#FS2)
1. [Before-read filtering or filter pushdown](#before-read-filtering-or-filter-pushdown)
1. [Schema projection](#schema-projection)
1. [Statistics](#Statistics)
1. [Supported storage types](#supported-storage-types)
1. [Supported types](#supported-types)
1. [Generic Records](#generic-records)
1. [Customisation and Extensibility](#customisation-and-extensibility)
1. [More Examples](#more-examples)
1. [Contributing](#contributing)

## Quick Start

### SBT

```scala
libraryDependencies ++= Seq(
  "com.github.mjakubowski84" %% "parquet4s-core" % "1.8.3",
  "org.apache.hadoop" % "hadoop-client" % yourHadoopVersion
)
```

### Mill

```scala
def ivyDeps = Agg(
  ivy"com.github.mjakubowski84::parquet4s-core:1.8.3",
  ivy"org.apache.hadoop:hadoop-client:$yourHadoopVersion"
)
```

```scala
import com.github.mjakubowski84.parquet4s.{ ParquetReader, ParquetWriter }

case class User(userId: String, name: String, created: java.sql.Timestamp)

val users: Iterable[User] = Seq(
  User("1", "parquet", new java.sql.Timestamp(1L))
)
val path = "path/to/local/parquet"

// writing
ParquetWriter.writeAndClose(path, users)

// reading
val parquetIterable = ParquetReader.read[User](path)
try {
  parquetIterable.foreach(println)
} finally parquetIterable.close()
```

## AWS S3

In order to connect to AWS S3 you need to define one more dependency:

```scala
"org.apache.hadoop" % "hadoop-aws" % yourHadoopVersion
```

Next, the most common way is to define following environmental variables:

```bash
export AWS_ACCESS_KEY_ID=my.aws.key
export AWS_SECRET_ACCESS_KEY=my.secret.key
```

Please follow [documentation of Hadoop AWS](https://hadoop.apache.org/docs/current/hadoop-aws/tools/hadoop-aws/index.html) for more details and troubleshooting.

### Passing Hadoop Configs Programmatically

File system configs for S3, GCS or Hadoop can also be set programmatically to the `ParquetReader` and `ParquetWriter` by passing the `Configuration` object to the `ParqetReader.Options` and `ParquetWriter.Options` case classes.  

## Akka Streams

Parquet4S has an integration module that allows you to read and write Parquet files using Akka Streams. Just import:

```scala
"com.github.mjakubowski84" %% "parquet4s-akka" % "1.8.3"
"org.apache.hadoop" % "hadoop-client" % yourHadoopVersion
```

Parquet4S has so far a single `Source` for reading single file or directory and `Sink`s for writing.

```scala
import com.github.mjakubowski84.parquet4s.{ParquetStreams, ParquetWriter}
import org.apache.parquet.hadoop.ParquetFileWriter
import org.apache.parquet.hadoop.metadata.CompressionCodecName
import akka.actor.ActorSystem
import akka.stream.{ActorMaterializer, Materializer}
import akka.stream.scaladsl.Source
import org.apache.hadoop.conf.Configuration
import scala.concurrent.duration._

case class User(userId: String, name: String, created: java.sql.Timestamp)

implicit val system: ActorSystem = ActorSystem()
implicit val materializer: Materializer = ActorMaterializer()

val users: Iterable[User] = ???

val conf: Configuration = ??? // Set Hadoop configuration programmatically

// Please check all the available configuration options!
val writeOptions = ParquetWriter.Options(
  writeMode = ParquetFileWriter.Mode.OVERWRITE,
  compressionCodecName = CompressionCodecName.SNAPPY,
  hadoopConf = conf // optional hadoopConf
)

// Writes a single file.
Source(users).runWith(ParquetStreams.toParquetSingleFile(
  path = "file:///data/users/user-303.parquet",
  options = writeOptions
))

// Tailored for writing indefinite streams.
// Writes file when chunk reaches size limit or defined time period elapses.
// Can also partition files!
// Check all the parameters and example usage in project sources.
Source(users).via(
  ParquetStreams
    .viaParquet[User]("file:///data/users")
    .withMaxCount(writeOptions.rowGroupSize)
    .withMaxDuration(30.seconds)
    .withWriteOptions(writeOptions)
    .build()
).runForeach(user => println(s"Just wrote user ${user.userId}..."))
  
// Reads a file or files from the path. Please also have a look at the rest of parameters.
ParquetStreams.fromParquet[User]
  .withOptions(ParquetReader.Options(hadoopConf = conf))
  .read("file:///data/users")
  .runForeach(println)
```

## FS2

FS2 integration allows you to read and write Parquet using functional streams. Functionality is exactly the same as in case of Akka module. In order to use it please import:

```scala
"com.github.mjakubowski84" %% "parquet4s-fs2" % "1.8.3"
"org.apache.hadoop" % "hadoop-client" % yourHadoopVersion
```

Please check [examples](./examples/src/main/scala/com/github/mjakubowski84/parquet4s/fs2) to learn more.

## Before-read filtering or filter pushdown

One of the best features of Parquet is an efficient way of fitering. Parquet files contain additional metadata that can be leveraged to drop chunks of data without scanning them. Parquet4S allows do define filter predicates in all modules in order to push filtering out from Scala collections and Akka or FS2 stream down to point before file content is even read.

You define you filters using simple algebra as follows.

In core library:

```scala
ParquetReader.read[User](path = "file://my/path", filter = Col("email") === "user@email.com")
```

In Akka filter applies both to content of files and partitions:

```scala
ParquetStreams.fromParquet[Stats]
  .withFilter(Col("stats.score") > 0.9 && Col("stats.score") <= 1.0)
  .read("file://my/path")
```

You can construct filter predicates using `===`, `!==`, `>`, `>=`, `<`, `<=`, and `in` operators on columns containing primitive values. You can combine and modify predicates using `&&`, `||` and `!` operators. `in` looks for values in a list of keys, similar to SQL's `in` operator. Mind that operations on `java.sql.Timestamp` and `java.time.LocalDateTime` are not supported as Parquet still not allows filtering by `Int96` out of the box.

Check ScalaDoc and code for more!

## Schema projection

Schema projection is another way of optimization of reads. By default Parquet4S reads the whole content of each Parquet record even when you provide a case class that maps only a part of the columns. Such a behaviour is expected because you may want to use [generic records](#generic-records) to process your data. However, you can explicitely tell Parquet4S to use the provided case class (or implicit `ParquetSchemaResolver`) as an override for the original file schema. In effect, all columns not matching your schema will be skipped and not read. This functionality is available in every module of Parquet4S.

```scala
// core
ParquetReader.withProjection[User].read(path = "file://my/path")

// akka
ParquetStreams.fromParquet[User].withProjection.read("file://my/path")

// fs2
import com.github.mjakubowski84.parquet4s.parquet._
fromParquet[IO, User].projection.read(blocker, "file://my/path")
```

## Statistics

Parquet4S leverages Parquet metadata to efficiently read record count as well as max and min value of the column of Parquet files. It provides correct value for both filtered and unfiltered files. Functionality is available in core module either by direct call to [Stats](src/main/scala/com/github/mjakubowski84/parquet4s/Stats.scala) or via API of `ParquetReader` and `ParquetIterable`.

## Supported storage types

As it is based on Hadoop Client, Parquet4S can read and write from a variety of file systems:

- Local files
- HDFS
- Amazon S3
- Google Storage
- Azure
- OpenStack

Please refer to Hadoop Client documentation or your storage provider to check how to connect to your storage.

## Supported types

### Primitive types

| Type                    | Reading and Writing | Filtering |
|:------------------------|:-------------------:|:---------:|
| Int                     | &#x2611;            | &#x2611;  |
| Long                    | &#x2611;            | &#x2611;  |
| Byte                    | &#x2611;            | &#x2611;  |
| Short                   | &#x2611;            | &#x2611;  |
| Boolean                 | &#x2611;            | &#x2611;  |
| Char                    | &#x2611;            | &#x2611;  |
| Float                   | &#x2611;            | &#x2611;  |
| Double                  | &#x2611;            | &#x2611;  |
| BigDecimal              | &#x2611;            | &#x2611;  |
| java.time.LocalDateTime | &#x2611;            | &#x2612;  |
| java.time.LocalDate     | &#x2611;            | &#x2611;  |
| java.sql.Timestamp      | &#x2611;            | &#x2612;  |
| java.sql.Date           | &#x2611;            | &#x2611;  |
| Array[Byte]             | &#x2611;            | &#x2611;  |

### Complex Types

Complex types can be arbitrarily nested.

- Option
- List
- Seq
- Vector
- Set
- Array - Array of bytes is treated as primitive binary
- Map - **Key must be of primitive type**, only **immutable** version.
- **Since 1.2.0**. Any Scala collection that has Scala 2.13 collection Factory (in 2.11 and 2.12 it is derived from CanBuildFrom). Refers to both mutable and immutable collections. Collection must be bounded only by one type of element - because of that Map is supported only in immutable version (for now).
- *Any case class*

## Generic Records

You may want to not use strict schema and process your data in a generic way. Since version 1.2.0 Parquet4S has rich API that allows to build, transform, write and read Parquet records in easy way. Each implementation of  `ParquetRecord` is Scala `Iterable` and a mutable collection. You can execute operations on `RowParquetRecord` and `ListParquetRecord` as on mutable `Seq` and you can treat `MapParquetRecord` as mutable `Map`. Moreover, records received addition functions like `get` and `add` (and more) that take implicit `ValueCodec` and allow to read and modify records using regular Scala types.  There is default `ParquetRecordEndcoder`, `ParquetRecordDecoder` and `ParquetSchemaResolver` for  `RowParquetRecord` so reading Parquet in a generic way works out of the box! In order to write you still need to provide a schema in form of Parquet's `MessageType`.

Funcionality is available in all modules. See [examples](https://github.com/mjakubowski84/parquet4s/blob/master/examples/src/main/scala/com/github/mjakubowski84/parquet4s/core/WriteAndReadGenericApp.scala).

## Customisation and Extensibility

Parquet4S is built using Scala's type class system. That allows you to extend Parquet4S by defining your own implementations of its type classes. 

For example, you may define your codecs of your own type so that they can be **read from or written** to Parquet. Assume that you have your own type:

```scala
case class CustomType(i: Int)
```

You want to save it as optional `Int`. In order to achieve you have to define your own codec:

```scala
import com.github.mjakubowski84.parquet4s.{OptionalValueCodec, IntValue, Value}

implicit val customTypeCodec: OptionalValueCodec[CustomType] = 
  new OptionalValueCodec[CustomType] {
    override protected def decodeNonNull(value: Value, configuration: ValueCodecConfiguration): CustomType = value match {
      case IntValue(i) => CustomType(i)
    }
    override protected def encodeNonNull(data: CustomType, configuration: ValueCodecConfiguration): Value =
      IntValue(data.i)
}
```

Additionally, if you want to write your custom type, you have to define the schema for it:

```scala
import org.apache.parquet.schema.{LogicalTypeAnnotation, PrimitiveType}
import com.github.mjakubowski84.parquet4s.ParquetSchemaResolver.TypedSchemaDef
import com.github.mjakubowski84.parquet4s.SchemaDef

implicit val customTypeSchema: TypedSchemaDef[CustomType] =
    SchemaDef.primitive(
      primitiveType = PrimitiveType.PrimitiveTypeName.INT32,
      required = false,
      originalType = Option(LogicalTypeAnnotation.intType(32, true))
    ).typed[CustomType]
```

## More Examples

Please check [examples](./examples) where you can find simple code covering basics for `core`, `akka` and `fs2` modules.

Moreover, examples contain two simple applications comprising Akka Streams or FS2 and Kafka. It shows how you can write partitioned Parquet files with data coming from an indefinite stream.

## Contributing

Do you want to contribute? Please read the [contribution guidelines](CONTRIBUTING.md).
