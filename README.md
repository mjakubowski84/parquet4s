# Parquet4S

Simple I/O for Parquet. Allows you to easily read and write Parquet files in Scala.

Use just Scala case class to define the schema of your data. No need to use Avro, Protobuf, Thrift or other data serialisation systems. You can use generic records if you don't want to use case class, too.

Compatible with files generated with Apache Spark. However, unlike in Spark, you do not have to start a cluster to perform I/O operations.

Based on official Parquet library, Hadoop Client and Shapeless.

Integration for Akka Streams.

Released for Scala 2.11.x, 2.12.x and 2.13.x.

## Supported storage types

As it is based on Hadoop Client Parquet4S can do read and write from variety of file systems starting from local files, HDFS to Amazon S3, Google Storage, Azure or OpenStack. Following you can find description how to read from local files and S3. Please refer to Hadoop Client documentation or your storage provider to check how to connect to your storage.

**Local files** are supported out of the box, no need to configure anything. Just provide provide a path to your file directory or use `file://` suffix in URI.

In order to connect to **S3** at **AWS** you need to import dependency:

```scala
"org.apache.hadoop" % "hadoop-aws" % yourHadoopVersion
```

Next, the most common way is to define following environmental variables:

```bash
export AWS_ACCESS_KEY_ID=my.aws.key
export AWS_SECRET_ACCESS_KEY=my.secret.key
```

Please follow [documentation of Hadoop AWS](https://hadoop.apache.org/docs/current/hadoop-aws/tools/hadoop-aws/index.html) for more details and troubleshooting.

#### Passing Hadoop Configs Programmatically 
File system configs for S3, GCS or Hadoop can also be set programmatically to the 
`ParquetReader` and `ParquetWriter` by passing the `Configuration` object to the 
`ParqetReader.Options` and `ParquetWriter.Options` case classes.  


## How to use Parquet4S to read and write parquet files?

### Core library

Add the library to your dependencies:

```scala
"com.github.mjakubowski84" %% "parquet4s-core" % "1.3.0"
```
**Note:** Since version `0.5.0` you need to define your own version of `hadoop-client`:
```scala
"org.apache.hadoop" % "hadoop-client" % yourHadoopVersion
```

The library contains simple implementation of Scala's Iterable that allows reading Parquet from a single file or a directory. You may also use `org.apache.parquet.hadoop.ParquetReader` directly and use our `RowParquetRecord` and `ParquetRecordDecoder` to decode your data.

```scala
import com.github.mjakubowski84.parquet4s.{ParquetReader, ParquetWriter}

case class User(userId: String, name: String, created: java.sql.Timestamp)

val users: Iterable[User] = ???
val path = "file:///data/users"

// writing
ParquetWriter.writeAndClose(path, users)

// reading
val parquetIterable = ParquetReader.read[User](path)
try {
  parquetIterable.foreach(println)
} finally parquetIterable.close()

```

### Akka Streams

Parquet4S has an integration module that allows you to read and write Parquet files using Akka Streams! Just import it:

```scala
"com.github.mjakubowski84" %% "parquet4s-akka" % "1.3.0"
```
**Note:** Since version `0.5.0` you need to define your own version of `hadoop-client`:
```scala
"org.apache.hadoop" % "hadoop-client" % yourHadoopVersion
```

Parquet4S has so far single `Source` for reading single file or directory and **four** `Sink`s for writing. Choose one that suits you most.

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

implicit val system: ActorSystem =  ActorSystem()
implicit val materializer: Materializer =  ActorMaterializer()

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

// Sequentially splits data into files of 'maxRecordsPerFile'.
// Recommended to use in environments with limitted available resources.
Source(users).runWith(ParquetStreams.toParquetSequentialWithFileSplit(
  path = "file:///data/users",
  // will create files consisting of max 2 row groups
  maxRecordsPerFile = 2 * writeOptions.rowGroupSize,
  options = writeOptions
))

// Writes files in parallel in number equal to 'parallelism'.
// Recommended to use in order to achieve better performance under condition that
// file order does not have to be preserved.
Source(users).runWith(ParquetStreams.toParquetParallelUnordered(
  path = "file:///data/users",
  parallelism = 4,
  options = writeOptions
))

// Tailored for writing indefinite streams.
// Writes file when chunk reaches size limit or defined time period elapses.
// Can also partition files!
// Check all other parameters and example usage in project sources.
Source(users).via(
  ParquetStreams
    .viaParquet[User]("file:///data/users")
    .withMaxCount(writeOptions.rowGroupSize)
    .withMaxDuration(30.seconds)
    .withWriteOptions(writeOptions)
		.build()
).runForeach(user => println(s"Just wrote user ${user.userId}..."))
  
// Reads a file or files from the path. Please also have a look at optional parameters.
ParquetStreams.fromParquet[User](
  path = "file:///data/users", 
  options = ParquetReader.Options(hadoopConf = conf)
).runForeach(println)
```

## Before-read filtering or filter pushdown

One of the best features of Parquet is efficient way of fitering. Parquet files contain additional metadata that can be leveraged to drop chunks of data without scanning them. Since version 0.10.0 Parquet4S allows do define filter predicates both in core and akka module in order to push filtering out from Scala collections or Akka Stream down to point before file content is even read.

You define you filters using simple algebra as follows.

In core library:

```scala
ParquetReader.read[User](path = "file://my/path", filter = Col("email") === "user@email.com")
```

In Akka filter applies both to content of files and partitions:

```scala
ParquetStreams.fromParquet[Stats](
  path = "file://my/path", 
  filter = Col("stats.score") > 0.9 && Col("stats.score") <= 1.0
)
```

> **TAKE NOTE!**
>
> Take note that due to an issue with implicit resolution in **Scala 2.11** you may need to define all parameters of `ParquetStreams.fromParquet` even if some have default values. Parameters must be specified in default order even when you use named arguments. It specifically refers to a case when you would like to omit `options` but define `filter`. Such a situation doesn't appear in Scala 2.12 and 2.13.

You can construct filter predicates using `===`, `!==`, `>`, `>=`, `<`, `<=`, and `in` operators on columns containing primitive values. You can combine and modify predicates using `&&`, `||` and `!` operators. `in` looks for values in a list of keys, similar to SQL's `in` operator. Mind that operations on `java.sql.Timestamp` and `java.time.LocalDateTime` are not supported as Parquet still not allows filtering by `Int96` out of the box.

Check ScalaDoc and code for more!

## Supported types

List of types that are supported out of the box for reading, writing and filtering can be found [here](supportedTypes.md).

## Generic Records

You may want to not use strict schema and process your data in a generic way. Since version 1.2.0 Parquet4S has rich API that allows to build, transform, write and read Parquet records in easy way. Each implementation of  `ParquetRecord` is Scala `Iterable` and a mutable collection. You can execute operations on `RowParquetRecord` and `ListParquetRecord` as on mutable `Seq` and you can treat `MapParquetRecord` as mutable `Map`. Moreover, records received addition functions like `get` and `add` (and more) that take implicit `ValueCodec` and allow to read and modify records using regular Scala types.  There is default `ParquetRecordEndcoder`, `ParquetRecordDecoder` and `ParquetSchemaResolver` for  `RowParquetRecord` so reading Parquet in a generic way works out of the box! In order to write you still need to provide a schema in form of Parquet's `MessageType`.

Funcionality is available both in core and Akka module. See [examples](https://github.com/mjakubowski84/parquet4s/blob/master/examples/src/main/scala/com/github/mjakubowski84/parquet4s/core/WriteAndReadGenericApp.scala).

## Customisation and extensibility

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
import org.apache.parquet.schema.{OriginalType, PrimitiveType}
import com.github.mjakubowski84.parquet4s.ParquetSchemaResolver._
 
implicit val customTypeSchema: TypedSchemaDef[CustomType] =
  typedSchemaDef[CustomType](
    PrimitiveSchemaDef(
      primitiveType = PrimitiveType.PrimitiveTypeName.INT32, 
      required = false, 
      originalType = Some(OriginalType.INT_32)
    )
  )
```

## Examples

Please check [examples](examples) where you can find simple code covering basics both for `core` and `akka` modules.

Moreover, examples contain a simple application of lib comprising Akka Streams and Kafka. It shows how you can write partitioned Parquet files with data coming from an indefinite stream.

## Contributing

Do you want to contribute? Please read the [contribution guidelines](CONTRIBUTING.md).
