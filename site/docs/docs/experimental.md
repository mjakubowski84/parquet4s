---
layout: docs
title: (Experimental) Joins and Concat
permalink: docs/experimental/
---

# (Experimental) Joins and Concat

Version 2.1.0 of Parquet4s introduces advanced operations on generic datasets, that is on `ParquetIterable[RowParquetRecord]`, to core module. Now users can join and concat two or more datasets what can simplify some ETL jobs a lot.

Available operations:

- Left join
- Right join
- Inner join
- Full join
- Concat (appending one dataset to another)

Mind that joins require loading right-side dataset into memory so that those operations are not applicable for very large datasets. Consider switching position of datasets in your join operation (left dataset is iterated over). Or use e.g. Apache Spark which distributes data across multiple machines for performing join operations.

Please note that this is an experimental feature. API may change in the future and some functionalities may be added or removed.

```scala mdoc:compile-only
import com.github.mjakubowski84.parquet4s.{Col, ParquetReader, Path}

case class PetOwner(id: Long, name: String, petId: Long, petName: String)

// define 1st dataset
val readOwners = ParquetReader
  .projectedGeneric(
    Col("id").as[Long],
    Col("name").as[String]
  )
  .read(Path("/owners"))

// define 2nd dataset
val readPets = ParquetReader
  .projectedGeneric(
    Col("id").as[Long].alias("petId"),
    Col("name").as[String].alias("petName"),
    Col("ownerId").as[Long]
  )
  .read(Path("/pets"))

// define join operation
val readPetOwners = readOwners
  .innerJoin(right = readPets, onLeft = Col("id"), onRight = Col("ownerId"))
  .as[PetOwner]

// execute
// note that all operations defined before are lazy and are not executed before `foreach` is ran
try readPetOwners.foreach(println)
finally readPetOwners.close()
```
