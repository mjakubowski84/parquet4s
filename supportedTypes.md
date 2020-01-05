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

Complex Types
-------------
**Only immutable types are supported**. Complex types can be arbitrarily nested.
 * Option
 * List
 * Seq
 * Vector
 * Set
 * Array
 * Map - **Key must be of primitive type***
 * *Any case class*
