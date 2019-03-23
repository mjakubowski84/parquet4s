package com.github.mjakubowski84.parquet4s

object TestCases {

  case class Empty()

  // Primitives
  case class Primitives(
                         boolean: Boolean,
                         int: Int,
                         long: Long,
                         float: Float,
                         double: Double,
                         string: String,
                         short: Short
                       )
  case class TimePrimitives(
                             localDateTime: java.time.LocalDateTime,
                             sqlTimestamp: java.sql.Timestamp,
                             localDate: java.time.LocalDate,
                             sqlDate: java.sql.Date
                           )
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
            array.sameElements(otherArray)
        case _ => false
      }
  }
  case class ContainsCollectionOfOptionalPrimitives(list: List[Option[Int]])
  case class ContainsCollectionOfCollections(listOfSets: List[Set[Int]])
  case class ContainsMapOfPrimitives(map: Map[String, Int])
  case class ContainsMapOfOptionalPrimitives(map: Map[String, Option[Int]])
  case class ContainsMapOfCollectionsOfPrimitives(map: Map[String, List[Int]])

  // Nested class
  case class Nested(int: Int)
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

}
