---
layout: docs
title: Records & Schema
permalink: docs/records_and_schema/
---
# Records

A data entry in Parquet is called a `record`. Record can represent a `row` of data, or it can be a nested complex field in another `row`. Another types of record are a `map` and a `list`. Stored data must be organised in `row`s. Neither primitive type nor `map` or `list` are allowed as a root data type.
In Parquet4s those concepts are represented by types that extend `ParquetRecord`: `RowParquetRecord` and `MapParquetRecord`.`ParquetRecord` extends Scala's immutable `Iterable` and allows iteration (next to many other operations) over its content: fields of a `row`, key-value entries of a `map` and elements of a `list`.

Parquet organizes row records into `pages` and pages into `row groups`. Row groups and pages are data blocks accompanied by metadata such as `statistics` and `dictionaries`. Metadata is leveraged during reading and filtering - so that some data blocks are skipped during reading if the metadata proves that the related data does not match provided filter predicate.

For more information about data structures in Parquet please refer to [official documentation](https://parquet.apache.org/documentation/latest/).

# Schema

Each Parquet file contains the schema of the data it stores. The schema defines structure of records, names and types of fields, optionality, etc. Schema is required for writing Parquet files and can be optionally used during reading (if you do not want to read all stored columns).

Official Parquet library, that Parquet4s is based on, defines the schema in Java type called `MessageType`. As it is quite tedious to define the schema and map it to your data types, Parquet4s comes with a handy mechanism that derives it automatically from Scala case classes. Please follow this documentation to learn which Scala types are supported out of the box and how to define custom encoders and decoders.

If you do not wish to map the schema of your data to Scala case classes then Parquet4s allows you to stick to generic records, that is, to aforementioned subtypes of `ParquetRecord`. Still, you must provide `MessageType` during writing. If you do not provide it during reading then Parquet4s uses the schema stored in a file and all its content is read. 

## Supported types

### Primitive types

| Type                    | Reading and Writing | Filtering |
|:------------------------|:-------------------:|:---------:|
| Int                     | &#x2611;            | &#x2611;  |
| Long                    | &#x2611;            | &#x2611;  |
| Byte                    | &#x2611;            | &#x2611;  |
| Short                   | &#x2611;            | &#x2611;  |
| Boolean                 | &#x2611;            | &#x2611;  |
| Char                    | &#x2611;            | &#x2611;  |
| Float                   | &#x2611;            | &#x2611;  |
| Double                  | &#x2611;            | &#x2611;  |
| BigDecimal              | &#x2611;            | &#x2611;  |
| java.time.LocalDateTime | &#x2611;            | &#x2612;  |
| java.time.LocalDate     | &#x2611;            | &#x2611;  |
| java.sql.Timestamp      | &#x2611;            | &#x2612;  |
| java.sql.Date           | &#x2611;            | &#x2611;  |
| Array[Byte]             | &#x2611;            | &#x2611;  |

### Complex Types

Complex types can be arbitrarily nested.

- Option
- List
- Seq
- Vector
- Set
- Array - Array of bytes is treated as primitive binary
- Map - **Key must be of primitive type**, only **immutable** version.
- Any Scala collection that has Scala collection `Factory` (in 2.12 it is derived from `CanBuildFrom`). Refers to both mutable and immutable collections. Collection must be bounded only by one type of element - because of that Map is supported only in immutable version.
- *Any case class*
