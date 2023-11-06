---
layout: docs
title: Projection
permalink: docs/projection/
---

# Projection

Schema projection is a way of optimization of reads. When calling `ParquetReader.as[MyData]` Parquet4s reads the whole content of each Parquet record even when you provide a case class that maps only a part of stored columns. The same happens when you use generic records by calling `ParquetReader.generic`. However, you can explicitly tell Parquet4s to use a different schema. In effect, all columns not matching your schema will be skipped and not read. You can define the projection schema in numerous ways:

1. by defining case class for typed read using `projectedAs`,
2. by defining generic column projection (allows reference to nested fields and aliases) using `projectedGeneric`,
3. by providing your own instance of Parquet's `MessageType` for generic read using `projectedGeneric`.

```scala mdoc:compile-only
import com.github.mjakubowski84.parquet4s.{Col, ParquetIterable, ParquetReader, Path, RowParquetRecord}
import org.apache.parquet.schema.MessageType

// typed read
case class MyData(column1: Int, columnX: String)
val myData: ParquetIterable[MyData] = 
  ParquetReader
    .projectedAs[MyData]
    .read(Path("file.parquet"))

// generic read with column projection
val records1: ParquetIterable[RowParquetRecord] = 
  ParquetReader
    .projectedGeneric(
      Col("column1").as[Int],
      Col("columnX").as[String].alias("my_column"),
    )
    .read(Path("file.parquet"))

// generic read with own instance of Parquet schema
val schemaOverride: MessageType = ???
val records2: ParquetIterable[RowParquetRecord] = 
  ParquetReader
    .projectedGeneric(schemaOverride)
    .read(Path("file.parquet"))
```

