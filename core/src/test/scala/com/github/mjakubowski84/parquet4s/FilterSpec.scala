package com.github.mjakubowski84.parquet4s

import java.time.{Instant, LocalDate, LocalDateTime, ZoneId}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.util.TimeZone

class FilterSpec extends AnyFlatSpec with Matchers {

  private val valueCodecConfiguration = ValueCodecConfiguration.apply(TimeZone.getTimeZone("UTC"))

  "Filter" should "build eq predicate" in {
    val predicate = (Col("i") === 1).toPredicate(valueCodecConfiguration)
    predicate.toString should be("eq(i, 1)")
  }

  it should "build neq predicate" in {
    val predicate = (Col("i") !== 1).toPredicate(valueCodecConfiguration)
    predicate.toString should be("noteq(i, 1)")
  }

  it should "build gt predicate" in {
    val predicate = (Col("i") > 1).toPredicate(valueCodecConfiguration)
    predicate.toString should be("gt(i, 1)")
  }

  it should "build gteq predicate" in {
    val predicate = (Col("i") >= 1).toPredicate(valueCodecConfiguration)
    predicate.toString should be("gteq(i, 1)")
  }

  it should "build lt predicate" in {
    val predicate = (Col("i") < 1).toPredicate(valueCodecConfiguration)
    predicate.toString should be("lt(i, 1)")
  }

  it should "build lteq predicate" in {
    val predicate = (Col("i") <= 1).toPredicate(valueCodecConfiguration)
    predicate.toString should be("lteq(i, 1)")
  }

  it should "build isNull predicate" in {
    val predicate = Col("i").isNull[Int].toPredicate(valueCodecConfiguration)
    predicate.toString should be("eq(i, null)")
  }

  it should "build isNotNull predicate" in {
    val predicate = Col("i").isNotNull[Int].toPredicate(valueCodecConfiguration)
    predicate.toString should be("not(eq(i, null))")
  }

  it should "build in predicate with varargs" in {
    val predicate = Col("i").in(1, 2, 3).toPredicate(valueCodecConfiguration)
    predicate.toString should be("userdefinedbyinstance(i, in(1, 2, 3))")
  }

  it should "build in predicate with collection" in {
    val predicate = (Col("i") in Seq(1, 2, 3)).toPredicate(valueCodecConfiguration)
    predicate.toString should be("userdefinedbyinstance(i, in(1, 2, 3))")
  }

  it should "build not predicate" in {
    // format: off
    val predicate = (!(Col("i") === 1)).toPredicate(valueCodecConfiguration)
    // format: on
    predicate.toString should be("not(eq(i, 1))")
  }

  it should "build and predicate" in {
    val predicate = (Col("i") === 1 && Col("j") > 0).toPredicate(valueCodecConfiguration)
    predicate.toString should be("and(eq(i, 1), gt(j, 0))")
  }

  it should "build or predicate" in {
    val predicate = (Col("i") === 1 || Col("i") === 2).toPredicate(valueCodecConfiguration)
    predicate.toString should be("or(eq(i, 1), eq(i, 2))")
  }

  it should "build predicate for int column" in {
    val predicate = (Col("i") === 1).toPredicate(valueCodecConfiguration)
    predicate.toString should be("eq(i, 1)")
  }

  it should "build predicate for long column" in {
    val predicate = (Col("l") === 1L).toPredicate(valueCodecConfiguration)
    predicate.toString should be("eq(l, 1)")
  }

  it should "build predicate for float column" in {
    val predicate = (Col("f") === 1.0f).toPredicate(valueCodecConfiguration)
    predicate.toString should be("eq(f, 1.0)")
  }

  it should "build predicate for double column" in {
    val predicate = (Col("d") === 1.0d).toPredicate(valueCodecConfiguration)
    predicate.toString should be("eq(d, 1.0)")
  }

  it should "build predicate for byte column" in {
    val predicate = (Col("b") === 1.toByte).toPredicate(valueCodecConfiguration)
    predicate.toString should be("eq(b, 1)")
  }

  it should "build predicate for short column" in {
    val predicate = (Col("s") === 1.toShort).toPredicate(valueCodecConfiguration)
    predicate.toString should be("eq(s, 1)")
  }

  it should "build predicate for char column" in {
    val predicate = (Col("ch") === 'x').toPredicate(valueCodecConfiguration)
    predicate.toString should be("eq(ch, 120)")
  }

  it should "build predicate for boolean column" in {
    val predicate = (Col("bool") === true).toPredicate(valueCodecConfiguration)
    predicate.toString should be("eq(bool, true)")
  }

  it should "build predicate for decimal column" in {
    val predicate = (Col("dec") === BigDecimal("0.0")).toPredicate(valueCodecConfiguration)
    predicate.toString should be("eq(dec, Binary{16 reused bytes, [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0]})")
  }

  it should "build predicate for java.time.LocalDate" in {
    val predicate = (Col("date") === LocalDate.of(1970, 1, 1)).toPredicate(valueCodecConfiguration)
    predicate.toString should be("eq(date, 0)")
  }

  it should "build predicate for java.sql.LocalDate" in {
    val predicate =
      (Col("date") === java.sql.Date.valueOf(LocalDate.of(1970, 1, 1))).toPredicate(valueCodecConfiguration)
    predicate.toString should be("eq(date, 0)")
  }

  it should "build predicate for java.time.LocalDateTime" in {
    import TimestampFormat.Implicits.Millis.*
    val dateTime  = LocalDateTime.ofInstant(Instant.ofEpochMilli(1L), ZoneId.of("UTC"))
    val predicate = (Col("dateTime") === dateTime).toPredicate(valueCodecConfiguration)
    predicate.toString should be("eq(dateTime, 1)")
  }

  it should "build predicate for java.sql.Timestamp" in {
    import TimestampFormat.Implicits.Millis.*
    val timestamp = java.sql.Timestamp.from(Instant.ofEpochMilli(1L))
    val predicate = (Col("dateTime") === timestamp).toPredicate(valueCodecConfiguration)
    predicate.toString should be("eq(dateTime, 1)")
  }

  it should "build udp" in {
    object MyUDP extends UDP[Int] {
      override def keep(value: Int): Boolean                                  = ???
      override def canDrop(statistics: FilterStatistics[Int]): Boolean        = ???
      override def inverseCanDrop(statistics: FilterStatistics[Int]): Boolean = ???
      override def name: String                                               = "MyUDP"
    }
    val predicate = Col("int").udp(MyUDP).toPredicate(valueCodecConfiguration)
    predicate.toString should be("userdefinedbyinstance(int, MyUDP)")
  }

}
