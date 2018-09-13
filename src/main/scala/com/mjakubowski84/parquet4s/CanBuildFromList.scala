package com.mjakubowski84.parquet4s

import scala.collection.generic.CanBuildFrom
import scala.collection.mutable
import scala.reflect.ClassTag

trait CanBuildFromList {

  implicit def cbfForArray[H : ClassTag]: CanBuildFrom[List[H], H, Array[H]] = {
    new CanBuildFrom[List[H], H, Array[H]] {
      override def apply(from: List[H]): mutable.Builder[H, Array[H]] = apply() ++= from
      override def apply(): mutable.Builder[H, Array[H]] = Array.newBuilder[H]
    }
  }

  implicit def cbfForSeq[H]: CanBuildFrom[List[H], H, Seq[H]] = {
    new CanBuildFrom[List[H], H, Seq[H]] {
      override def apply(from: List[H]): mutable.Builder[H, Seq[H]] = apply() ++= from
      override def apply(): mutable.Builder[H, Seq[H]] = Seq.newBuilder[H]
    }
  }

  implicit def cbfForSet[H]: CanBuildFrom[List[H], H, Set[H]] = {
    new CanBuildFrom[List[H], H, Set[H]] {
      override def apply(from: List[H]): mutable.Builder[H, Set[H]] = apply() ++= from
      override def apply(): mutable.Builder[H, Set[H]] = Set.newBuilder[H]
    }
  }

  implicit def cbfForOption[H]: CanBuildFrom[List[H], H, Option[H]] = {
    new CanBuildFrom[List[H], H, Option[H]] {
      override def apply(from: List[H]): mutable.Builder[H, Option[H]] = {
        require(from.size <= 1, s"Input list have size of ${from.size} what is more than Option can handle.")
        from.headOption.foldLeft(apply())((b, elem) => b += elem)
      }
      override def apply(): mutable.Builder[H, Option[H]] = new mutable.Builder[H, Option[H]]  {
        private var content: Option[H] = None
        override def +=(elem: H): this.type = {
          assert(content.isEmpty, s"Cannot add $elem to the option, option has value already assigned")
          content = Some(elem)
          this
        }
        override def clear(): Unit = ()
        override def result(): Option[H] = content
      }
    }
  }

}
