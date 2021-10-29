---
layout: docs
title: Records, types and schema
permalink: docs/records_and_schema/
---
# Records

A data entry in Parquet is called a `record`. Record can represent a `row` of data, or it can be a nested complex field in another `row`. Another types of record are a `map` and a `list`. Stored data must be organised in `row`s. Neither primitive type nor `map` or `list` are allowed as a root data type.
In Parquet4s those concepts are represented by types that extend `ParquetRecord`: `RowParquetRecord`, `MapParquetRecord` and `ListParquetRecord`. `ParquetRecord` extends Scala's immutable `Iterable` and allows iteration (next to many other operations) over its content: fields of a `row`, key-value entries of a `map` and elements of a `list`. When using the library you have an option to use those data structures directly, or you can use regular Scala classes that are encoded/decoded by instances of `ValueCodec` to/from `ParqutRecord`.

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

### Custom Types

Parquet4s is built using Scala's type class system. That allows you to extend Parquet4s by defining your own implementations of type classes.

For example, you may define a codec for your own type so that it can be read from or written to Parquet. Assuming that you have your own type:

```scala
case class CustomType(i: Int)
```

You want to save it as optional `Int`. In order to achieve that you have to define a codec:

```scala mdoc:compile-only
import com.github.mjakubowski84.parquet4s.{OptionalValueCodec, IntValue, Value, ValueCodecConfiguration}

case class CustomType(i: Int)

implicit val customTypeCodec: OptionalValueCodec[CustomType] = 
  new OptionalValueCodec[CustomType] {
    override protected def decodeNonNull(value: Value, configuration: ValueCodecConfiguration): CustomType = value match {
      case IntValue(i) => CustomType(i)
    }
    override protected def encodeNonNull(data: CustomType, configuration: ValueCodecConfiguration): Value =
      IntValue(data.i)
}
```

Additionally, if you want to write your custom type, you have to define the schema for it:

```scala mdoc:compile-only
import org.apache.parquet.schema.{LogicalTypeAnnotation, PrimitiveType}
import com.github.mjakubowski84.parquet4s.ParquetSchemaResolver.TypedSchemaDef
import com.github.mjakubowski84.parquet4s.{LogicalTypes, SchemaDef}

case class CustomType(i: Int)

implicit val customTypeSchema: TypedSchemaDef[CustomType] =
  SchemaDef.primitive(
    primitiveType = PrimitiveType.PrimitiveTypeName.INT32,
    required = false,
    logicalTypeAnnotation = Option(LogicalTypes.Int32Type)
  ).typed[CustomType]
```

In order to filter by a field of a custom type `T` you have to implement `FilterCodec[T]` type class.

```scala mdoc:compile-only
import com.github.mjakubowski84.parquet4s.FilterCodec
import org.apache.parquet.filter2.predicate.Operators.IntColumn

case class CustomType(i: Int)

implicit val customFilterCodec: FilterCodec[CustomType, java.lang.Integer, IntColumn] =
  FilterCodec[CustomType, java.lang.Integer, IntColumn](
    encode = (customType, _) => customType.i,
    decode = (integer, _)    => CustomType(integer)
  )
```

# Using generic records directly

Parquet4s allows you to choose to use generic records explicitly from the level of API in each module of the library. But you can also use typed API and define `RowParquetRecord` as your data type. Parquet4s contains type classes for encoding, decoding and automatic schema resolution for `RowParquetRecord`.

```scala mdoc:compile-only
import com.github.mjakubowski84.parquet4s.{ParquetReader, ParquetWriter, Path, RowParquetRecord}
import org.apache.parquet.schema.MessageType

// both reads are equivalent
ParquetReader.generic.read(Path("file.parquet"))
ParquetReader.as[RowParquetRecord].read(Path("file.parquet"))

val data: Iterable[RowParquetRecord] = ???
// when using generic record you need to define the schema on your own
implicit val schema: MessageType = ???

// both writes are equivalent
ParquetWriter
  .generic(schema) // schema is passed explicitly
  .writeAndClose(Path("file.parquet"), data)
ParquetWriter
  .of[RowParquetRecord] // schema is passed implicitly
  .writeAndClose(Path("file.parquet"), data)
```