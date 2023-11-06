---
layout: docs
title: Quick start
permalink: docs/quick_start/
---

# Quick start

## SBT

```scala
libraryDependencies ++= Seq(
  "com.github.mjakubowski84" %% "parquet4s-core" % "@VERSION@",
  "org.apache.hadoop" % "hadoop-client" % yourHadoopVersion
)
```

## Mill

```scala
def ivyDeps = Agg(
  ivy"com.github.mjakubowski84::parquet4s-core:@VERSION@",
  ivy"org.apache.hadoop:hadoop-client:$yourHadoopVersion"
)
```

```scala mdoc:compile-only
import com.github.mjakubowski84.parquet4s.{ ParquetReader, ParquetWriter, Path }

case class User(userId: String, name: String, created: java.sql.Timestamp)

val users: Iterable[User] = Seq(
  User("1", "parquet", new java.sql.Timestamp(1L))
)
val path = Path("path/to/local/file.parquet")

// writing
ParquetWriter.of[User].writeAndClose(path, users)

// reading
val parquetIterable = ParquetReader.as[User].read(path)
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

You may need to set some configuration properties to access your storage, e.g. `fs.s3a.path.style.access`. 
Please follow [documentation of Hadoop AWS](https://hadoop.apache.org/docs/current/hadoop-aws/tools/hadoop-aws/index.html) for more details and troubleshooting.

## Passing Hadoop Configs Programmatically

File system configs for S3, GCS, Hadoop, etc. can also be set programmatically to the `ParquetReader` and `ParquetWriter` by passing the `Configuration` to the `ParqetReader.Options` and `ParquetWriter.Options` case classes.  

```scala mdoc:compile-only
import com.github.mjakubowski84.parquet4s.{ ParquetReader, ParquetWriter, Path }
import org.apache.parquet.hadoop.metadata.CompressionCodecName
import org.apache.hadoop.conf.Configuration

case class User(userId: String, name: String, created: java.sql.Timestamp)

val users: Iterable[User] = Seq(
  User("1", "parquet", new java.sql.Timestamp(1L))
)

val hadoopConf = new Configuration()
hadoopConf.set("fs.s3a.path.style.access", "true")

val writerOptions = ParquetWriter.Options(
  compressionCodecName = CompressionCodecName.SNAPPY,
  hadoopConf = hadoopConf
)

ParquetWriter
  .of[User]
  .options(writerOptions)
  .writeAndClose(Path("path/to/local/file.parquet"), users)
```
