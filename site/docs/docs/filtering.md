---
layout: docs
title: Filtering
permalink: docs/filtering/
---

# Filtering

One of the best features of Parquet is an efficient way of filtering. Parquet files contain additional metadata that can be leveraged to drop chunks of data without scanning them. Parquet4s allows to define a filter predicates in order to push filtering out from Scala collections and Akka or FS2 stream down to a point before file content is even read.

In Akka and FS2 filter applies both to content of files and [partitions]({% link docs/partitioning.md %})).

You define your filters using simple algebra as follows:

```scala mdoc:compile-only
import com.github.mjakubowski84.parquet4s.{Col, ParquetReader, Path}

case class User(id: Long, email: String, visits: Long)

ParquetReader
  .as[User]
  .filter(Col("email") === "user@email.com" && Col("visits") >= 100)
  .read(Path("file.parquet"))
```

You can construct filter predicates using `===`, `!==`, `>`, `>=`, `<`, `<=`, `in`  and  `udp` operators on columns containing primitive values. You can combine and modify predicates using `&&`, `||` and `!` operators. `in` looks for values in a list of keys, similar to SQL's `in` operator. Mind that operations on `java.sql.Timestamp` and `java.time.LocalDateTime` are not supported as Parquet still does not allow filtering by `Int96` columns. 

For custom filtering by column of type `T` implement `UDP[T]` trait and use `udp` operator.

```scala
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
