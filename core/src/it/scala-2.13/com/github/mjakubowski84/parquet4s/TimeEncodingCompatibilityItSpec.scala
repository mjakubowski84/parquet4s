package com.github.mjakubowski84.parquet4s

import com.github.mjakubowski84.parquet4s.CompatibilityTestCases.TimePrimitives
import com.github.mjakubowski84.parquet4s.TimeValueCodecs.*
import org.scalatest.BeforeAndAfter
import org.scalatest.freespec.AnyFreeSpecLike
import org.scalatest.matchers.should.Matchers

import java.sql.Date
import java.time.{LocalDate, LocalDateTime}
import java.util.TimeZone

abstract class TimeEncodingCompatibilityItSpec
    extends AnyFreeSpecLike
    with Matchers
    with BeforeAndAfter
    with SparkHelper {

  private val newYearMidnight = LocalDateTime.of(2019, 1, 1, 0, 0, 0)
  private val newYear         = Date.valueOf(LocalDate.of(2019, 1, 1))
  private val timeZones = List(
    TimeZone.getTimeZone("GMT-1"),
    TimeZone.getTimeZone("UTC"),
    TimeZone.getTimeZone("GMT+1")
  )

  protected val parquetWriter: ParquetWriter.Builder[TimePrimitives] = ParquetWriter.of[TimePrimitives]

  before {
    clearTemp()
  }

  private def writeWithSpark(data: TimePrimitives): Unit = writeToTemp(Seq(data))

  private def readWithSpark: TimePrimitives = readFromTemp[TimePrimitives].head

  private def writeWithParquet4S(data: TimePrimitives, timeZone: TimeZone): Unit =
    parquetWriter
      .options(ParquetWriter.Options(timeZone = timeZone))
      .writeAndClose(tempPath, Seq(data))

  private def readWithParquet4S(timeZone: TimeZone): TimePrimitives = {
    val parquetIterable =
      ParquetReader.as[TimePrimitives].options(ParquetReader.Options(timeZone = timeZone)).read(tempPath)
    try parquetIterable.head
    finally parquetIterable.close()
  }

  "For time zone of" - {
    timeZones.foreach { timeZone =>
      val data = TimePrimitives(timestamp = localDateTimeToTimestamp(newYearMidnight, timeZone), date = newYear)
      timeZone.getDisplayName - {
        "Spark should read data written by Parquet4s" in {
          writeWithParquet4S(data, timeZone)
          readWithSpark should be(data)
        }
        "Parquet4s should read data written by Spark" in {
          writeWithSpark(data)
          readWithParquet4S(timeZone) should be(data)
        }
      }
    }
  }

}
