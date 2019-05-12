# Parquet4S

Simple I/O for Parquet. Allows you to easily read and write Parquet files in Scala.

Use just Scala case class to define the schema of your data. No need to use Avro, Protobuf, Thrift or other data serialisation systems.

Compatible with files generated with Apache Spark. However, unlike in Spark, you do not have to start a cluster to perform I/O operatotions.

Based on official Parquet library, Hadoop Client and Shapeless.

Integration for Akka Streams.

## Supported storages

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

## How to use Parquet4S to read and write parquet files?

### Core library

Add the library to your dependencies:

```scala
"com.github.mjakubowski84" %% "parquet4s-core" % "0.5.0"
```
**Note:** Since version `0.5.0` you need to define your own version of `hadoop-client`:
```scala
"org.apache.hadoop" % "hadoop-client" % yourHadoopVersion
```

The library contains simple implementation of Scala's Iterable that allows reading Parquet from a single file or a directory. You may also use `org.apache.parquet.hadoop.ParquetReader` directly and use our `RowParquetRecord` and `ParquetRecordDecoder` to decode your data.

```scala
import com.github.mjakubowski84.parquet4s.{ParquetReader, ParquetWriter}

case class User(userId: String, name: String, created: java.sql.Timestamp)

val users: Stream[User] = ???
val path = "file:///data/users"

// writing
ParquetWriter.write(path, users)

// reading
val parquetIterable = ParquetReader.read[User](path)
try {
  parquetIterable.foreach(println)
} finally parquetIterable.close()
```

### Akka Streams

Parquet4S has an integration module that allows you to read and write Parquet files using Akka Streams! Just import it:

```scala
"com.github.mjakubowski84" %% "parquet4s-akka" % "0.5.0"
```
**Note:** Since version `0.5.0` you need to define your own version of `hadoop-client`:
```scala
"org.apache.hadoop" % "hadoop-client" % yourHadoopVersion
```

Parquet4S has so far single `Source` for reading single file or directory and **three** `Sink`s for writing. Choose one that suits you most.

```scala
import com.github.mjakubowski84.parquet4s.{ParquetStreams, ParquetWriter}
import org.apache.parquet.hadoop.ParquetFileWriter
import org.apache.parquet.hadoop.metadata.CompressionCodecName
import akka.actor.ActorSystem
import akka.stream.{ActorMaterializer, Materializer}
import akka.stream.scaladsl.Source

case class User(userId: String, name: String, created: java.sql.Timestamp)

implicit val system: AcrtorSystem =  ActorSystem()
implicit val materializer: Materializer =  ActorMaterializer()

val users: Stream[User] = ???

// Please check all the available configuration options!
val writeOptions = ParquetWriter.Options(
  writeMode = ParquetFileWriter.Mode.OVERWRITE,
  compressionCodecName = CompressionCodecName.SNAPPY
)

// Writes a single file.
Source(users).runWith(ParquetStreams.toParquetSingleFile(
  path = "file:///data/users/user-303.parquet",
  options = writeOptions
))

// Sequentially splits data into files of 'maxRecordsPerFile'.
// Recommended to use in environments with limitted available resources.
Source(data).runWith(ParquetStreams.toParquetSequentialWithFileSplit(
  path = "file:///data/users",
  // will create files consisting of max 2 row groups
  maxRecordsPerFile = 2 * writeOptions.rowGroupSize,
  options = writeOptions
))

// Writes files in parallel in number equal to 'parallelism'.
// Recommended to use in order to achieve better performance under condition that
// file order does not have to be preserved.
Source(data).runWith(ParquetStreams.toParquetParallelUnordered(
  path = "file:///data/users",
  parallelism = 4,
  options = writeOptions
))
  
// Reads file or files from the path. Please also have a look at optional parameters.
ParquetStreams.fromParquet[User]("file:///data/users").runForeach(println)
```

## Customisation and extensibility

Parquet4S is built using Scala's type class system. That allows you to extend Parquet4S by defining your own implementations of its type classes. 

For example, you may define your codecs of your own type so that they can be read fromor written to Parquet. Assume that you have your own type:

```scala
case class CustomType(string: String)
```

You want to save it as regular optional `String`. In order to achieve you have to define your own codec:

```scala
import com.github.mjakubowski84.parquet4s.{OptionalValueCodec, StringValue, Value}

implicit val customTypeCodec: OptionalValueCodec[CustomType] = 
  new OptionalValueCodec[CustomType] {
    override protected def decodeNonNull(value: Value, configuration: ValueCodecConfiguration): CustomType = value match {
      case StringValue(string) => CustomType(string)
    }
    override protected def encodeNonNull(data: CustomType, configuration: ValueCodecConfiguration): Value =
      StringValue(data.string)
}
```

Additionaly, if you want to write your custom type, you have to define the schema for it:

```scala
import org.apache.parquet.schema.{OriginalType, PrimitiveType}
import com.github.mjakubowski84.parquet4s.ParquetSchemaResolver._
 
implicit val customTypeSchema: TypedSchemaDef[CustomType] =
  typedSchemaDef[CustomType](
    PrimitiveSchemaDef(
      primitiveType = PrimitiveType.PrimitiveTypeName.BINARY, 
      required = false, 
      originalType = Some(OriginalType.UTF8)
    )
  )
```

## Contributing

Do you want to contribute? Please read the [contribution guidelines](CONTRIBUTING.md).
