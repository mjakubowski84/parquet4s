package com.github.mjakubowski84.parquet4s

import TimeValueCodecs.localDateTimeToTimestamp

import java.util.TimeZone

object CompatibilityTestCases extends TestCaseSupport {

  private val timeZone = TimeZone.getTimeZone("UTC")

  // Primitives
  case class Primitives(
      b: Boolean,
      i: Int,
      l: Long,
      f: Float,
      d: Double,
      s: String,
      sh: Short,
      by: Byte,
      bd: BigDecimal
  )
  case class TimePrimitives(timestamp: java.sql.Timestamp, date: java.sql.Date)
  case class OtherPrimitives(ch: Char, date: java.time.LocalDate, dateTime: java.time.LocalDateTime)
  case class ContainsOption(optional: Option[Int])

  // Collections of primitives
  case class Collections(
      list: List[Int],
      seq: Seq[Int],
      vector: Vector[Int],
      set: Set[Int],
      array: Array[Int]
  ) {
    override def equals(obj: Any): Boolean =
      obj match {
        case other @ Collections(otherList, otherSeq, otherVector, otherSet, otherArray) =>
          (other canEqual this) &&
            list == otherList &&
            seq == otherSeq &&
            vector == otherVector &&
            set == otherSet &&
            ((array == null && otherArray == null) || array.sameElements(otherArray))
        case _ => false
      }
  }
  case class CollectionsOfStrings(
      list: List[String],
      seq: Seq[String],
      vector: Vector[String],
      set: Set[String],
      array: Array[String]
  ) {
    override def equals(obj: Any): Boolean =
      obj match {
        case other @ CollectionsOfStrings(otherList, otherSeq, otherVector, otherSet, otherArray) =>
          (other canEqual this) &&
            list == otherList &&
            seq == otherSeq &&
            vector == otherVector &&
            set == otherSet &&
            ((array == null && otherArray == null) || array.sameElements(otherArray))
        case _ => false
      }
  }
  case class ArrayOfBytes(bytes: Array[Byte]) {
    override def equals(obj: Any): Boolean =
      obj match {
        case other @ ArrayOfBytes(null) =>
          (other canEqual this) && this.bytes == null
        case other @ ArrayOfBytes(bytes) =>
          (other canEqual this) && this.bytes.sameElements(bytes)
        case _ => false
      }
  }
  case class ContainsCollectionOfOptionalPrimitives(list: List[Option[Int]])
  case class ContainsCollectionOfCollections(listOfSets: List[Set[Int]])
  case class ContainsMapOfPrimitives(map: Map[String, Int])
  case class ContainsReversedMapOfPrimitives(map: Map[Int, String])
  case class ContainsMapOfOptionalPrimitives(map: Map[String, Option[Int]])
  case class ContainsMapOfCollectionsOfPrimitives(map: Map[String, List[Int]])

  // Nested class
  case class Nested(i: Int)
  case class ContainsNestedClass(nested: Nested)
  case class ContainsOptionalNestedClass(nestedOptional: Option[Nested])

  // Collections of nested class
  case class CollectionsOfNestedClass(
      list: List[Nested],
      seq: Seq[Nested],
      vector: Vector[Nested],
      set: Set[Nested],
      array: Array[Nested]
  ) {
    override def equals(obj: Any): Boolean =
      obj match {
        case other @ CollectionsOfNestedClass(otherList, otherSeq, otherVector, otherSet, otherArray) =>
          (other canEqual this) &&
            list == otherList &&
            seq == otherSeq &&
            vector == otherVector &&
            set == otherSet &&
            array.sameElements(otherArray)
        case _ => false
      }
  }
  case class ContainsMapOfNestedClassAsValue(nested: Map[String, Nested])
  case class ContainsMapOfNestedClassAsKey(nested: Map[Nested, String])
  case class ContainsMapOfOptionalNestedClassAsValue(nested: Map[String, Option[Nested]])
  case class ContainsMapOfCollectionsOfNestedClassAsValue(nested: Map[String, List[Nested]])

  override val caseDefinitions: Seq[Case.CaseDef] = Seq(
    Case(
      "primitives",
      Seq(
        Primitives(b = true, 1, 1234567890L, 1.1f, 1.00000000001d, "text", 1, 1, BigDecimal("1.0000000000000000")),
        Primitives(b = false, 0, 0L, 0f, 0d, "", 0, 0, BigDecimal("0.0000000000000000")),
        Primitives(
          b = false,
          Int.MaxValue,
          Long.MaxValue,
          Float.MaxValue,
          Double.MaxValue,
          "Żołądź z dębu",
          Short.MaxValue,
          Byte.MaxValue,
          BigDecimal("0.0000000000000001")
        ),
        Primitives(
          b = false,
          Int.MinValue,
          Long.MinValue,
          Float.MinValue,
          Double.MinValue,
          null,
          Short.MinValue,
          Byte.MinValue,
          BigDecimal("-0.0000000000000001")
        )
      )
    ),
    Case(
      "time primitives",
      Seq(
        TimePrimitives(
          timestamp = localDateTimeToTimestamp(java.time.LocalDateTime.of(2019, 1, 1, 0, 0, 0), timeZone),
          date      = java.sql.Date.valueOf(java.time.LocalDate.of(2019, 1, 1))
        ),
        TimePrimitives(
          timestamp = localDateTimeToTimestamp(java.time.LocalDateTime.of(2019, 1, 1, 0, 30, 0, 2000), timeZone),
          date      = java.sql.Date.valueOf(java.time.LocalDate.of(2019, 1, 1))
        ),
        TimePrimitives(
          timestamp = localDateTimeToTimestamp(java.time.LocalDateTime.of(2018, 12, 31, 23, 30, 0, 999000), timeZone),
          date      = java.sql.Date.valueOf(java.time.LocalDate.of(2018, 12, 31))
        ),
        TimePrimitives(timestamp = null, date = null)
      )
    ),
    Case(
      "other primitives",
      Seq(
        OtherPrimitives(
          ch       = 0,
          date     = java.time.LocalDate.of(2019, 1, 1),
          dateTime = java.time.LocalDateTime.of(2019, 1, 1, 0, 0, 0)
        ),
        OtherPrimitives(
          ch       = 'a',
          date     = java.time.LocalDate.of(2019, 1, 1),
          dateTime = java.time.LocalDateTime.of(2018, 12, 31, 23, 30, 0, 999000)
        ),
        OtherPrimitives(
          ch       = '\r',
          date     = null,
          dateTime = null
        )
      ),
      Set(CompatibilityParty.Reader, CompatibilityParty.Writer)
    ),
    Case("options", Seq(ContainsOption(None), ContainsOption(Some(1)))),
    Case(
      "collections of primitives",
      Seq(
        Collections(
          list   = List.empty,
          seq    = Seq.empty,
          vector = Vector.empty,
          set    = Set.empty,
          array  = Array.empty
        ),
        Collections(
          list   = null,
          seq    = null,
          vector = null,
          set    = null,
          array  = null
        ),
        Collections(
          list   = List(1, 2, 3),
          seq    = Seq(1, 2, 3),
          vector = Vector(1, 2, 3),
          set    = Set(1, 2, 3),
          array  = Array(1, 2, 3)
        )
      )
    ),
    Case(
      "collections of strings",
      Seq(
        CollectionsOfStrings(
          list   = List.empty,
          seq    = Seq.empty,
          vector = Vector.empty,
          set    = Set.empty,
          array  = Array.empty
        ),
        CollectionsOfStrings(
          list   = null,
          seq    = null,
          vector = null,
          set    = null,
          array  = null
        ),
        CollectionsOfStrings(
          list   = List("a", "b", "c"),
          seq    = Seq("a", "b", "c"),
          vector = Vector("a", "b", "c"),
          set    = Set("a", "b", "c"),
          array  = Array("a", "b", "c")
        ),
        CollectionsOfStrings(
          list   = List("a", null, "c"),
          seq    = Seq(null, "b", null),
          vector = Vector(null, null, null),
          set    = Set("a", "b", "c"),
          array  = Array("a", "b", "c")
        )
      )
    ),
    Case(
      "array of bytes",
      Seq(
        ArrayOfBytes(bytes = null),
        ArrayOfBytes(bytes = Array.empty),
        ArrayOfBytes(bytes = Array(1, 2, 3))
      )
    ),
    Case(
      "collection with optional primitives",
      Seq(
        ContainsCollectionOfOptionalPrimitives(List.empty),
        ContainsCollectionOfOptionalPrimitives(List(None, Some(2), None))
      )
    ),
    Case(
      "map of primitives",
      Seq(
        ContainsMapOfPrimitives(Map.empty),
        ContainsMapOfPrimitives(null),
        ContainsMapOfPrimitives(Map("key_1" -> 1, "key_2" -> 2, "key_3" -> 3))
      )
    ),
    Case(
      "reversed map of primitives",
      Seq(
        ContainsReversedMapOfPrimitives(Map.empty),
        ContainsReversedMapOfPrimitives(null),
        ContainsReversedMapOfPrimitives(Map(1 -> "a", 2 -> "b", 3 -> "c")),
        ContainsReversedMapOfPrimitives(Map(1 -> "a", 2 -> null, 3 -> "c"))
      )
    ),
    Case(
      "map of optional primitives",
      Seq(
        ContainsMapOfOptionalPrimitives(Map.empty),
        ContainsMapOfOptionalPrimitives(Map("1" -> None, "2" -> Some(2)))
      )
    ),
    Case(
      "map of collections of primitives",
      Seq(
        ContainsMapOfCollectionsOfPrimitives(Map.empty),
        ContainsMapOfCollectionsOfPrimitives(Map("1" -> List.empty, "2" -> List(1, 2, 3)))
      )
    ),
    Case(
      "nested class",
      Seq(
        ContainsNestedClass(Nested(1)),
        ContainsNestedClass(null)
      )
    ),
    Case(
      "nested optional class",
      Seq(
        ContainsOptionalNestedClass(None),
        ContainsOptionalNestedClass(Some(Nested(1)))
      )
    ),
    Case(
      "collection of nested class",
      Seq(
        CollectionsOfNestedClass(
          list   = List.empty,
          seq    = Seq.empty,
          vector = Vector.empty,
          set    = Set.empty,
          array  = Array.empty
        ),
        CollectionsOfNestedClass(
          list   = List(Nested(1), Nested(2), Nested(3)),
          seq    = Seq(Nested(1), Nested(2), Nested(3)),
          vector = Vector(Nested(1), Nested(2), Nested(3)),
          set    = Set(Nested(1), Nested(2), Nested(3)),
          array  = Array(Nested(1), Nested(2), Nested(3))
        ),
        CollectionsOfNestedClass(
          list   = List(null, Nested(2), Nested(3)),
          seq    = Seq(Nested(1), null, Nested(3)),
          vector = Vector(Nested(1), Nested(2), null),
          set    = Set(Nested(1), null, Nested(3)),
          array  = Array(null, null, null)
        )
      )
    ),
    Case(
      "map of nested class as value",
      Seq(
        ContainsMapOfNestedClassAsValue(Map.empty),
        ContainsMapOfNestedClassAsValue(Map("1" -> Nested(1), "2" -> Nested(2))),
        ContainsMapOfNestedClassAsValue(Map("1" -> null, "2" -> Nested(2)))
      )
    ),
    Case(
      "map of nested class as key",
      Seq(
        ContainsMapOfNestedClassAsKey(Map.empty),
        ContainsMapOfNestedClassAsKey(Map(Nested(1) -> "1", Nested(2) -> "2")),
        ContainsMapOfNestedClassAsKey(Map(Nested(1) -> null))
      ),
      compatibilityParties = Set(CompatibilityParty.Reader, CompatibilityParty.Writer)
    ),
    Case(
      "map of optional nested class as value",
      Seq(
        ContainsMapOfOptionalNestedClassAsValue(Map.empty),
        ContainsMapOfOptionalNestedClassAsValue(
          Map(
            "none" -> None,
            "some" -> Some(Nested(2))
          )
        )
      )
    ),
    Case(
      "map of collections of nested class as value",
      Seq(
        ContainsMapOfCollectionsOfNestedClassAsValue(Map.empty),
        ContainsMapOfCollectionsOfNestedClassAsValue(
          Map(
            "empty" -> List.empty,
            "nonEmpty" -> List(Nested(1), Nested(2), Nested(3)),
            "contains single null" -> List(Nested(1), null, Nested(3)),
            "contains only nulls" -> List(null, null, null)
          )
        )
      )
    )
  )

}
