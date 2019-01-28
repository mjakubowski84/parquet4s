package com.github.mjakubowski84.parquet4s


object CompatibilityTestCases extends TestCaseSupport {

  // Primitives
  case class Primitives(
                         b: Boolean,
                         i: Int,
                         l: Long,
                         f: Float,
                         d: Double,
                         s: String
                       )
  case class TimePrimitives(timestamp: java.sql.Timestamp, date: java.sql.Date)
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
  case class ContainsMapOfNestedClass(nested: Map[String, Nested])
  case class ContainsMapOfOptionalNestedClass(nested: Map[String, Option[Nested]])
  case class ContainsMapOfCollectionsOfNestedClass(nested: Map[String, List[Nested]])


  override val caseDefinitions: Seq[Case.CaseDef] = Seq(
    Case("primitives", Seq(
      Primitives(b = true, 1, 1234567890l, 1.1f, 1.00000000001d, "text")
    )),
//    Case("time primitives", Seq(
//      TimePrimitives(
//        timestamp = java.sql.Timestamp.valueOf(java.time.LocalDateTime.of(2019, 1, 1, 12, 0, 1)),
//        date = java.sql.Date.valueOf(java.time.LocalDate.of(2019, 1, 1))
//      )
//    ), Set(CompatibilityParty.Spark, CompatibilityParty.Reader)),
    Case("options", Seq(ContainsOption(None), ContainsOption(Some(1)))),
    Case("collections of primitives", Seq(
      Collections(
        list = List.empty, seq = Seq.empty, vector = Vector.empty, set = Set.empty, array = Array.empty
      ),
      Collections(
        list = List(1, 2, 3),
        seq = Seq(1, 2, 3),
        vector = Vector(1, 2, 3),
        set = Set(1, 2, 3),
        array = Array(1, 2, 3)
      )
    )),
    Case("collection with optional primitives", Seq(
      ContainsCollectionOfOptionalPrimitives(List.empty),
      ContainsCollectionOfOptionalPrimitives(List(None, Some(2), None))
    )),
    Case("map of primitives", Seq(
      ContainsMapOfPrimitives(Map.empty),
      ContainsMapOfPrimitives(Map("key_1" -> 1, "key_2" -> 2, "key_3" -> 3))
    )),
    Case("map of optional primitives", Seq(
      ContainsMapOfOptionalPrimitives(Map.empty),
      ContainsMapOfOptionalPrimitives(Map("1" -> None, "2" -> Some(2)))
    )),
    Case("map of collections of primitives", Seq(
      ContainsMapOfCollectionsOfPrimitives(Map.empty),
      ContainsMapOfCollectionsOfPrimitives(Map("1" -> List.empty, "2" -> List(1, 2, 3)))
    )),
    Case("nested class", Seq(
      ContainsNestedClass(Nested(1))
    )),
    Case("nested optional class", Seq(
      ContainsOptionalNestedClass(None),
      ContainsOptionalNestedClass(Some(Nested(1)))
    )),
    Case("collection of nested class", Seq(
      CollectionsOfNestedClass(
        list = List.empty, seq = Seq.empty, vector = Vector.empty, set = Set.empty, array = Array.empty
      ),
      CollectionsOfNestedClass(
        list = List(Nested(1), Nested(2), Nested(3)),
        seq = Seq(Nested(1), Nested(2), Nested(3)),
        vector = Vector(Nested(1), Nested(2), Nested(3)),
        set = Set(Nested(1), Nested(2), Nested(3)),
        array = Array(Nested(1), Nested(2), Nested(3))
      )
    )),
    Case("map of nested class", Seq(
      ContainsMapOfNestedClass(Map.empty),
      ContainsMapOfNestedClass(Map("1" -> Nested(1), "2" -> Nested(2)))
    )),
    Case("map of optional nested class", Seq(
      ContainsMapOfOptionalNestedClass(Map.empty),
      ContainsMapOfOptionalNestedClass(Map(
        "none" -> None,
        "some" -> Some(Nested(2))
      ))
    )),
    Case("map of collections of nested class", Seq(
      ContainsMapOfCollectionsOfNestedClass(Map.empty),
      ContainsMapOfCollectionsOfNestedClass(Map(
        "empty" -> List.empty,
        "nonEmpty" -> List(Nested(1), Nested(2), Nested(3))
      ))
    ))
  )

}
