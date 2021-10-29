---
layout: docs
title: Projection
permalink: docs/projection/
---

# Projection

Schema projection is a way of optimization of reads. When calling `ParquetReader.as[MyData]` Parquet4s reads the whole content of each Parquet record even when you provide a case class that maps only a part of stored columns. The same happens when you use generic records by calling `ParquetReader.generic`. However, you can explicitly tell Parquet4s to use the provided case class or provided `MessageType` as an override for the original file schema. In effect, all columns not matching your schema will be skipped and not read.

```scala mdoc:compile-only
import com.github.mjakubowski84.parquet4s.{ParquetIterable, ParquetReader, Path, RowParquetRecord}
import org.apache.parquet.schema.MessageType

// typed read
case class MyData(column1: Int, columnX: String)
val myData: ParquetIterable[MyData] = 
  ParquetReader
    .projectedAs[MyData]
    .read(Path("file.parquet"))

// generic read
val schemaOverride: MessageType = ???
val records: ParquetIterable[RowParquetRecord] = 
  ParquetReader
    .projectedGeneric(schemaOverride)
    .read(Path("file.parquet"))
```

