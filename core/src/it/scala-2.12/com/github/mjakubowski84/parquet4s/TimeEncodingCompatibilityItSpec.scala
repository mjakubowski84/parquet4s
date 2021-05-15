package com.github.mjakubowski84.parquet4s

import java.util.TimeZone

import com.github.mjakubowski84.parquet4s.CompatibilityTestCases.TimePrimitives
import org.scalatest.BeforeAndAfter
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class TimeEncodingCompatibilityItSpec extends
  AnyFlatSpec
    with Matchers
    with BeforeAndAfter
    with SparkHelper {

  private val localTimeZone = TimeZone.getDefault
  private val utcTimeZone = TimeZone.getTimeZone("UTC")
  private lazy val newYearEveEvening = java.sql.Timestamp.valueOf(java.time.LocalDateTime.of(2018, 12, 31, 23, 0, 0))
  private lazy val newYearMidnight = java.sql.Timestamp.valueOf(java.time.LocalDateTime.of(2019, 1, 1, 0, 0, 0))
  private lazy val newYear = java.sql.Date.valueOf(java.time.LocalDate.of(2019, 1, 1))

  override def beforeAll(): Unit = {
    super.beforeAll()
    TimeZone.setDefault(utcTimeZone)
  }

  before {
    clearTemp()
  }

  private def writeWithSpark(data: TimePrimitives): Unit = writeToTemp(Seq(data))
  private def readWithSpark: TimePrimitives = readFromTemp[TimePrimitives].head
  private def writeWithParquet4S(data: TimePrimitives, timeZone: TimeZone): Unit =
    ParquetWriter.writeAndClose(tempPath, Seq(data), ParquetWriter.Options(timeZone = timeZone))
  private def readWithParquet4S(timeZone: TimeZone): TimePrimitives = {
    val parquetIterable = ParquetReader.read[TimePrimitives](tempPath, ParquetReader.Options(timeZone = timeZone))
    try {
      parquetIterable.head
    } finally {
      parquetIterable.close()
    }
  }

  "Spark" should "read properly time written with time zone one hour east" in {
    val input = TimePrimitives(timestamp = newYearMidnight, date = newYear)
    val expectedOutput = TimePrimitives(timestamp = newYearEveEvening, date = newYear)
    writeWithParquet4S(input, TimeZone.getTimeZone("GMT+1"))
    readWithSpark should be(expectedOutput)
  }

  it should "read properly written with time zone one hour west" in {
    val input = TimePrimitives(timestamp = newYearEveEvening, date = newYear)
    val expectedOutput = TimePrimitives(timestamp = newYearMidnight, date = newYear)
    writeWithParquet4S(input, TimeZone.getTimeZone("GMT-1"))
    readWithSpark should be(expectedOutput)
  }

  "Parquet4S" should "read properly time written with time zone one hour east" in {
    val input = TimePrimitives(timestamp = newYearMidnight, date = newYear)
    val expectedOutput = TimePrimitives(timestamp = newYearEveEvening, date = newYear)
    writeWithSpark(input)
    readWithParquet4S(TimeZone.getTimeZone("GMT-1")) should be(expectedOutput)
  }

  it should "read properly time written with time zone one hour west" in {
    val input = TimePrimitives(timestamp = newYearEveEvening, date = newYear)
    val expectedOutput = TimePrimitives(timestamp = newYearMidnight, date = newYear)
    writeWithSpark(input)
    readWithParquet4S(TimeZone.getTimeZone("GMT+1")) should be(expectedOutput)
  }

  override def afterAll(): Unit = {
    TimeZone.setDefault(localTimeZone)
    super.afterAll()
  }

}
