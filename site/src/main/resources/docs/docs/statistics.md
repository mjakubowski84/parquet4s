---
layout: docs
title: Statistics
permalink: docs/statistics/
---

# Statistics

Parquet files contain metadata that are used to optimize [filtering]({% link docs/filtering.md %}). Additionally, Parquet4s leverages metadata to provide insight about datasets in an efficient way:

- Number of records
- Min value of a column
- Max value of a column

Parquet4s will try to resolve those statistics without iterating over each record if possible. Statistics can also be queried using a filter — but please mind that speed of the query might decrease as, due to filtering, the algorithm might need to iterate over the content of a row group to resolve min/max values. The performance of the query is the best in the case of sorted datasets.

Parquet4s provides separate API for Statistics. It is also leveraged in `ParqueIterable`e.g. to efficiently calculate `size`.

```scala mdoc:compile-only
import com.github.mjakubowski84.parquet4s.{Col, Path, Stats}

import java.time.LocalDate
case class User(id: Long, age: Int, registered: LocalDate)

// stats of users that registered in year 2020
val userStats = Stats
  .builder
  .filter(Col("registered") >= LocalDate.of(2020, 1, 1) && Col("registered") < LocalDate.of(2021, 1, 1))
  .projection[User]
  .stats(Path("users"))

val numberOfUsers = userStats.recordCount
val minAge = userStats.min[Int](Col("age"))
val maxAge = userStats.max[Int](Col("age"))
```

```scala mdoc:compile-only
import com.github.mjakubowski84.parquet4s.{Col, ParquetReader, Path, Stats}

import java.time.LocalDate
case class User(id: Long, age: Int, registered: LocalDate)

// users that registered in year 2020
val users = ParquetReader
  .projectedAs[User]
  .filter(Col("registered") >= LocalDate.of(2020, 1, 1) && Col("registered") < LocalDate.of(2021, 1, 1))
  .read(Path("users"))

try {
  val numberOfUsers = users.size
  val minAge = users.min[Int](Col("age"))
  val maxAge = users.max[Int](Col("age"))
} finally {
  users.close()
}
```
