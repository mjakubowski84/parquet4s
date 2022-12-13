---
layout: docs
title: Migration from 1.x
permalink: docs/migration/
---

# Migration from 1.x

## Records

In 1.x `ParquetRecord` and its implementations were mutable `Iterable`s.\
In 2.x those classes are immutable. That is, any modification on a record returns a new instance.

In 1.x when reading using generic records `RowParquetRecord` had no entries for missing optional fields.\
In 2.x each field is represented in `RowParquerRecord`, the order is kept, and `NullValue` represents missing data.

## Type classes

`SkippingParquetSchemaResolver` and `SkippingParquetRecordEncoder` are removed in 2.x and their logic is merged into regular `ParquetSchemaResolver` and `ParquetRecordEncoder`.

`PartitionLens` is removed in 2.x. Its functionality is now available in the API of `RowParquetRecord`.

`ValueCodec` now composes `ValueDecoder` and `ValueEncoder` which allows writing custom encoders or decoders without implementing both.

### Core API changes

#### Reading
Was:
```scala
ParquetReader
  .read[Data](
    path = "path/to/file", 
    options = ParquetReader.Options(), 
    filter = Col("id") > 100
  )
```

Is:
```scala
ParquetReader
  .as[Data]
  .options(ParquetReader.Options())
  .filter(Col("id") > 100)
  .read(Path("path/to/file"))
```

#### Reading with projection
Was:
```scala
ParquetReader
  .withProjection[Data]
  .read(
    path = "path/to/file", 
    options = ParquetReader.Options(), 
    filter = Col("id") > 100
  )
```

Is:
```scala
ParquetReader
  .projectedAs[Data]
  .options(ParquetReader.Options())
  .filter(Col("id") > 100)
  .read(Path("path/to/file"))
```

#### Reading generic records
Was:
```scala
ParquetReader
  .read[RowParquetRecord](
    path = "path/to/file", 
    options = ParquetReader.Options(), 
    filter = Col("id") > 100
  )
```

Is:
```scala
ParquetReader
  .generic
  .options(ParquetReader.Options())
  .filter(Col("id") > 100)
  .read(Path("path/to/file"))
```

Or:
```scala
ParquetReader
  .as[RowParquetRecord]
  .options(ParquetReader.Options())
  .filter(Col("id") > 100)
  .read(Path("path/to/file"))
```

### Writing
Was:
```scala
ParquetWriter.writeAndClose(
  path = "path/to/file", 
  data = data, 
  options = ParquetWriter.Options()
)
```

Is:
```scala
ParquetWriter
  .of[Data]
  .options(ParquetWriter.Options())
  .writeAndClose(Path("path/to/file"), data)
```

### Writing generic records
Was:
```scala
implicit val schema: MessageType = ???
ParquetWriter.writeAndClose(
  path = "path/to/file",
  data = records, 
  options = ParquetWriter.Options()
)
```

Is:
```scala
ParquetWriter
  .generic(schema)
  .options(ParquetWriter.Options())
  .writeAndClose(Path("path/to/file"), records)
```

Or:
```scala
implicit val schema: MessageType = ???
ParquetWriter
  .of[RowParquetRecord]
  .options(ParquetWriter.Options())
  .writeAndClose(Path("path/to/file"), records)
```

### Stats
Was:
```scala
Stats(
  path = "path/to/file", 
  options = ParquetReader.Options(), 
  filter = Col("id") > 100
)
```

Is:
```scala
Stats
  .builder
  .options(ParquetReader.Options())
  .filter(Col("id") > 100)
  .stats(Path("path/to/file"))
```

### Akka API changes

Changes related to generic records are the same as in the core library.

Deprecated API of `fromParquet`, `toParquetSequentialWithFileSplit`, `toParquetParallelUnordered` and `toParquetIndefinite` is removed in 2.x.

#### Reading
Was:
```scala
ParquetStreams
  .fromParquet[Data]
  .withOptions(ParquetReader.Options())
  .withFilter(Col("id") > 100)
  .withProjection
  .read("path/to/file")
```

Is:
```scala
ParquetStreams
  .fromParquet
  .projectedAs[Data]
  .options(ParquetReader.Options())
  .filter(Col("id") > 100)
  .read(Path("path/to/file"))
```

Note that type class `SkippingParquetSchemaResolver` that was required for projection is now replaced by regular `ParquetSchemaResolver`.

### Writing single file

Was:
```scala
ParquetStreams
  .toParquetSingleFile(
    path = "path/to/file",
    options = ParquetWriter.Options()
  )
```

Is:
```scala
ParquetStreams
  .toParquetSingleFile
  .of[Data]
  .options(ParquetWriter.Options())
  .write(Path("path/to/file"))
```

### Advanced writing

Was:
```scala
ParquetStreams
  .viaParquet[User]("path/to/directory")
  .withMaxCount(1024 * 1024)
  .withMaxDuration(30.seconds)
  .withWriteOptions(ParquetWriter.Options())
  .withPartitionBy("col1", "col2")
  .build()
```

Is:
```scala
ParquetStreams
  .viaParquet
  .of[Data]
  .maxCount(1024 * 1024)
  .maxDuration(30.seconds)
  .options(ParquetWriter.Options())
  .partitionBy(Col("col1"), Col("col2"))
  .write(Path("path/to/directory"))
```

In 1.x rotation was executed when `maxCount` was reached *and* when `maxDuration` expired.\
In 2.x rotation is executed when `maxCount` is reached *or* when `maxDuration` expires. The counter and timer are reset after each rotation.

In 1.x all files (all partitions) were rotated at once.\
In 2.x each file (each partition) is rotated individually.

In 1.x `preWriteTransformation` could produce only a single record.\
In 2.x `preWriteTransformation` can produce a collection of records.

In 1.x `postWriteHandler` allowed the implementation of custom rotation of all files.\
In 2.x `postWriteHandler` allows the implementation of custom rotation of individual files.

Please note the dependency to type class `PartitionLens` is removed and `SkippingParquetSchemaResolver` and `SkippingParquetRecordEncoder` are replaced by regular `ParquetSchemaResolver` and `ParquetRecordEncoder`. 

### FS2 API changes

Changes related to generic records are the same as in the core library.

In 2.x FS2 and Cats Effect are upgraded to version 3.x.

Deprecated API of `read` is removed in 2.x

#### Reading
Was:
```scala
parquet
  .fromParquet[IO, Data]
  .options(ParquetReader.Options())
  .filter(Col("id") > 100)
  .projection
  .read(blocker, "path/to/file")
```

Is:
```scala
parquet
  .fromParquet[IO]
  .projectedAs[Data]
  .options(ParquetReader.Options())
  .filter(Col("id") > 100)
  .read(Path("path/to/file"))
```

Note that type class `SkippingParquetSchemaResolver` that was required for projection is now replaced by regular `ParquetSchemaResolver`.

### Writing single file

Was:
```scala
parquet
  .writeSingleFile[IO, Data](
    blocker = blocker,
    path = "path/to/file",
    options = ParquetWriter.Options()
  )
```

Is:
```scala
parquet
  .writeSingleFile[IO]
  .of[Data]
  .options(ParquetWriter.Options())
  .write(Path("path/to/file"))
```

### Advanced writing

Was:
```scala
parquet
  .viaParquet[IO, User]
  .maxCount(1024 * 1024)
  .maxDuration(30.seconds)
  .options(ParquetWriter.Options())
  .partitionBy("col1", "col2")
  .write(blocker, "path/to/directory")
```

Is:
```scala
parquet
  .viaParquet[IO]
  .of[Data]
  .maxCount(1024 * 1024)
  .maxDuration(30.seconds)
  .options(ParquetWriter.Options())
  .partitionBy(Col("col1"), Col("col2"))
  .write(Path("path/to/directory"))
```

In 1.x rotation was executed when `maxCount` was reached *and* when `maxDuration` expired.\
In 2.x rotation is executed when `maxCount` is reached *or* when `maxDuration` expires. The counter and timer are reset after each rotation.

In 1.x all files (all partitions) were rotated at once.\
In 2.x each file (each partition) is rotated individually.

In 1.x `postWriteHandler` allowed the implementation of custom rotation of all files.\
In 2.x `postWriteHandler` allows the implementation of custom rotation of individual files.

Please note the dependency to type class `SkippingParquetSchemaResolver` is replaced by regular `ParquetSchemaResolver`. 
