# Parquet4S

Simple Scala reader of Parquet files. 

Spark not needed anymore to just read Parquet. 

Use just Scala case class to define the schema of your data.
No need to use Avro, Protobuf, Thrift or other data serialisation
systems.

Compatible with files generated with Apache Spark.

Based on official Parquet library, Hadoop Client and Shapeless.

# How to use Parquet4S to read parquet files?

Add the library to your dependencies:

```
"com.github.mjakubowski84" %% "parquet4s-core" % "0.2.0"

```

The library contains simple implementation of Scala's Iterable that allows reading Parquet from a single file or a directory.
You may also use `org.apache.parquet.hadoop.ParquetReader` directly and use our `RowParquetRecord` and `ParquetRecordDecoder`
to decode your data.

Library supports by default connection to your local files and hdfs. There's also plenty of connectors to other systems like
Google Storage, Amazon's S3, Azure, OpenStack. Please look at Hadoop's or your storage provider's documentation.
Following you can see examples how to read Parquet from local files or AWS S3.

## Local files

```scala
import com.github.mjakubowski84.parquet4s.ParquetReader
import com.github.mjakubowski84.parquet4s.ParquetRecordDecoder._

case class User(userId: String, name: String, created: java.sql.Timestamp)

val parquetIterable = ParquetReader[User]("file:///data/users")
parquetIterable.foreach(println)
parquetIterable.close()

```

## AWS S3

Add Hadoop's library that enables reading files from S3:

```
"org.apache.hadoop" % "hadoop-aws" % "2.9.1"

```

Configure credentials according to documentation at 
[Hadoop's website](https://hadoop.apache.org/docs/current/hadoop-aws/tools/hadoop-aws/index.html#S3A_Authentication_methods).
For example define following environmental variables:
```bash
export AWS_ACCESS_KEY_ID=my.aws.key
export AWS_SECRET_ACCESS_KEY=my.secret.key
```

And then just use the iterable:

```scala
import com.github.mjakubowski84.parquet4s.ParquetReader
import com.github.mjakubowski84.parquet4s.ParquetRecordDecoder._

case class Data(id: Int, name: String, description: String)

val parquetIterable = ParquetReader[Data]("s3a:/my-bucket/data")
parquetIterable.foreach(println)
parquetIterable.close()

```

# How to use Parquet4S with Akka Streams?

Parquet4S has a simple integration module that allows you to read Parquet file using Akka Streams!
Just import it:

```
"com.github.mjakubowski84" %% "parquet4s-akka" % "0.2.0"

```

And now you can define Akka Streams source that reads Parquet files:

```scala
import com.github.mjakubowski84.parquet4s.ParquetStreams
import com.github.mjakubowski84.parquet4s.ParquetRecordDecoder._
import akka.actor.ActorSystem
import akka.stream.{ActorMaterializer, Materializer}

case class User(userId: String, name: String, created: java.sql.Timestamp)

implicit val system: AcrtorSystem =  ActorSystem()
implicit val materializer: Materializer =  ActorMaterializer()

ParquetStreams.fromParquet[User]("file:///data/users").runForeach(println)

system.terminate()

```
