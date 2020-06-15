Primitive types
---------------
| Type                    | Reading and Writing | Filtering |
|:-----------------------:|:-------------------:|:---------:|
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

Complex Types
-------------
Complex types can be arbitrarily nested.
 * Option
 * List
 * Seq
 * Vector
 * Set
 * Array - Array of bytes is treated as primitive binary
 * Map - **Key must be of primitive type**, only **immutable** version. 
 * **Since 1.2.0**. Any Scala collection that has Scala 2.13 collection Factory (in 2.11 and 2.12 it is derived from CanBuildFrom). Refers to both mutable and immutable collections. Collection must be bounded only by one type of element - because of that Map is supported only in immutable version (for now).
 * *Any case class*
 
