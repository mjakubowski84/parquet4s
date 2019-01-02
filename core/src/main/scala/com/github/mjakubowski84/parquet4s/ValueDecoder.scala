package com.github.mjakubowski84.parquet4s

import java.nio.{ByteBuffer, ByteOrder}
import java.util.TimeZone

import scala.collection.generic.CanBuildFrom
import scala.language.higherKinds


trait ValueDecoder[T] {
  def decode(s: Any): T
}

trait PrimitiveValueDecoders {
  implicit val stringDecoder: ValueDecoder[String] = new ValueDecoder[String] {
    override def decode(s: Any): String = s.toString
  }

  implicit val booleanDecoder: ValueDecoder[Boolean] = new ValueDecoder[Boolean] {
    override def decode(s: Any): Boolean = {
      s match {
        case b : Boolean => b
      }
    }
  }

  implicit val intDecoder: ValueDecoder[Int] = new ValueDecoder[Int] {
    override def decode(s: Any): Int = {
      s match {
        case int : Int => int
        case long : Long => long.toInt
      }
    }
  }

  implicit val longDecoder: ValueDecoder[Long] = new ValueDecoder[Long] {
    override def decode(s: Any): Long = {
      s match {
        case int : Int => int.toLong
        case long : Long => long
      }
    }
  }

  implicit val doubleDecoder: ValueDecoder[Double] = new ValueDecoder[Double] {
    override def decode(s: Any): Double = {
      s match {
        case double : Double => double
        case float : Float => float.toDouble
      }
    }
  }

  implicit val floatDecoder: ValueDecoder[Float] = new ValueDecoder[Float] {
    override def decode(s: Any): Float = {
      s match {
        case double : Double => double.toFloat
        case float : Float => float
      }
    }
  }
}

trait TimeValueDecoders {

  /**
    * Uses decoding that is implemented in Apache Spark.
    */
  implicit val timestampDecoder: ValueDecoder[java.sql.Timestamp] = new ValueDecoder[java.sql.Timestamp] {

    override def decode(s: Any): java.sql.Timestamp = {
      s match {
        case t: java.sql.Timestamp =>
          t
        case d: java.sql.Date =>
          java.sql.Timestamp.from(d.toInstant)
        case bs: Array[Byte] =>
          val buf = ByteBuffer.wrap(bs).order(ByteOrder.LITTLE_ENDIAN)
          val timeOfDayNanos = buf.getLong
          val julianDay = buf.getInt
          val rawTime = DateTimeUtils.fromJulianDay(julianDay, timeOfDayNanos)
          new java.sql.Timestamp(DateTimeUtils.toMillis(rawTime))
      }
    }
  }

  /**
    * Uses decoding that is implemented in Apache Spark.
    */
  implicit val dateDecoder: ValueDecoder[java.sql.Date] = new ValueDecoder[java.sql.Date] {

    override def decode(s: Any): java.sql.Date = {
      s match {
        case t: java.sql.Timestamp =>
          java.sql.Date.valueOf(t.toLocalDateTime.toLocalDate)
        case d: java.sql.Date =>
          d
        case daysSinceEpoch : java.lang.Integer =>
          new java.sql.Date(DateTimeUtils.daysToMillis(daysSinceEpoch, TimeZone.getDefault))
      }
    }
  }

}

trait CollectionValueDecoders {

  implicit def collectionDecoder[T, Col[_]](implicit t: ValueDecoder[T], cbf: CanBuildFrom[List[T], T, Col[T]]): ValueDecoder[Col[T]] = new ValueDecoder[Col[T]] {
    override def decode(s: Any): Col[T] = {
      s match {
        case listRecord: ListParquetRecord =>
          listRecord.getList.map(t.decode).to[Col]
        case other: T =>
          List(other).to[Col]
      }
    }
  }

  implicit def mapDecoder[K, V](implicit kt: ValueDecoder[K], vt: ValueDecoder[V]): ValueDecoder[Map[K, V]] = new ValueDecoder[Map[K, V]] {
    override def decode(s: Any): Map[K, V] = {
      s match {
        case mapParquetRecord: MapParquetRecord =>
          mapParquetRecord.getMap.map { case (key, value) =>
            kt.decode(key) -> vt.decode(value)
          }
      }
    }
  }

}

trait AllValueDecoders
  extends PrimitiveValueDecoders
  with TimeValueDecoders
  with CollectionValueDecoders
