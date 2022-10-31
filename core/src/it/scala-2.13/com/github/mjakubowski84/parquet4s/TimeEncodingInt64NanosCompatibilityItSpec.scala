package com.github.mjakubowski84.parquet4s

import com.github.mjakubowski84.parquet4s.CompatibilityTestCases.TimePrimitives
import com.github.mjakubowski84.parquet4s.TimeValueCodecs.localDateTimeToTimestamp
import org.scalatest.BeforeAndAfter
import org.scalatest.freespec.AnyFreeSpec
import org.scalatest.matchers.should.Matchers

import java.sql.Date
import java.time.{LocalDate, LocalDateTime}
import java.util.TimeZone
import TimestampFormat.Implicits.Nanos.*

class TimeEncodingInt64NanosCompatibilityItSpec extends AnyFreeSpec with Matchers with BeforeAndAfter with TestUtils {

  private val newYearMidnight = LocalDateTime.of(2019, 1, 1, 0, 0, 0)
  private val newYear         = Date.valueOf(LocalDate.of(2019, 1, 1))
  private val timeZones = List(
    TimeZone.getTimeZone("GMT-1"),
    TimeZone.getTimeZone("UTC"),
    TimeZone.getTimeZone("GMT+1")
  )

  before {
    clearTemp()
  }

  private def writeWithParquet4S(data: TimePrimitives, timeZone: TimeZone): Unit =
    ParquetWriter
      .of[TimePrimitives]
      .options(ParquetWriter.Options(timeZone = timeZone))
      .writeAndClose(tempPath, Seq(data))

  private def readWithParquet4S(timeZone: TimeZone): TimePrimitives = {
    val parquetIterable =
      ParquetReader.as[TimePrimitives].options(ParquetReader.Options(timeZone = timeZone)).read(tempPath)
    try parquetIterable.head
    finally parquetIterable.close()
  }

  "Parquet4s should read data written with time zone of" - {
    timeZones.foreach { timeZone =>
      val data = TimePrimitives(timestamp = localDateTimeToTimestamp(newYearMidnight, timeZone), date = newYear)
      timeZone.getDisplayName in {
        writeWithParquet4S(data, timeZone)
        readWithParquet4S(timeZone) should be(data)
      }
    }
  }

}
