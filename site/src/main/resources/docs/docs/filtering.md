---
layout: docs
title: Filtering
permalink: docs/filtering/
---

# Filtering

One of the best features of Parquet is an efficient way of filtering. Parquet files contain additional metadata that can be leveraged to drop chunks of data without scanning them. Parquet4s allows users to define filter predicates in order to push filtering out from Scala collections and Akka / Pekko or FS2 stream down to a point before file content is even read.

In Akka / Pekko and FS2 filter applies both to the content of files and [partitions]({% link docs/partitioning.md %}).

You define your filters using simple algebra as follows:

```scala mdoc:compile-only
import com.github.mjakubowski84.parquet4s.{Col, ParquetReader, Path}

case class User(id: Long, email: String, visits: Long)

ParquetReader
  .as[User]
  .filter(Col("email") === "user@email.com" && Col("visits") >= 100)
  .read(Path("file.parquet"))
```

You can construct filter predicates using `===`, `!==`, `>`, `>=`, `<`, `<=`, `in`, `isNull`, `isNotNull` and `udp` operators on columns containing primitive values. You can combine and modify predicates using `&&`, `||` and `!` operators. `in` looks for values in a list of keys, similar to SQL's `in` operator. Please mind that filtering on `java.sql.Timestamp` and `java.time.LocalDateTime` is not supported for `Int96` timestamps which is a default type used for timestamps. Consider a different timestamp [format]({% link docs/records_and_schema.md %}) for your data to enable filtering.

For custom filtering by a column of type `T` implement `UDP[T]` trait and use `udp` operator.

```scala mdoc:compile-only
import com.github.mjakubowski84.parquet4s.{Col, FilterStatistics, ParquetReader, Path, UDP}

case class MyRecord(int: Int)

object IntDividesBy10 extends UDP[Int] {
  private val Ten = 10
  
  // Called for each individual row that belongs to a row group that passed row group filtering.
  override def keep(value: Int): Boolean = value % Ten == 0
  
  // Called for each row group.
  // It should contain a logic that eliminates a whole row group if statistics prove that it doesn't contain 
  // data matching the predicate.
  @inline
  override def canDrop(statistics: FilterStatistics[Int]): Boolean = {
    val minMod = statistics.min % Ten
    val maxMod = statistics.max % Ten
    (statistics.max - statistics.min < Ten) && maxMod >= minMod
  }
  
  // Called for each row group for "not" predicates. The logic might be different than one in `canDrop`.
  override def inverseCanDrop(statistics: FilterStatistics[Int]): Boolean = !canDrop(statistics)
  
  // called by `toString`
  override val name: String = "IntDividesBy10"
}

ParquetReader
  .as[MyRecord]
  .filter(Col("int").udp(IntDividesBy10))
  .read(Path("my_ints.parquet"))
```

## Record filter \[experimental\]

`RecordFilter` is an alternative to filter predicates. It allows to filter records based on record index, that is an ordinal of a record in the file.

```scala mdoc:compile-only
import com.github.mjakubowski84.parquet4s.{ParquetReader, Path, RecordFilter}

case class User(id: Long, email: String, visits: Long)

// skips first 10 users
ParquetReader
  .as[User]
  .filter(RecordFilter(index => index >= 10))
  .read(Path("file.parquet"))
```
