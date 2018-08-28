package com.mjakubowski84.parquet4s

import java.util.TimeZone

/**
  * This is an extract from the object of the same name published in Spark Project Catalyst library in version 2.3.1
  * under ASF license.
  *
  * Function 'daysToMillis' is simplified and does not contain complex offset calculation as in the original and uses
  * just raw offset from timezone parameter. The rest of functions and fields is kept as in original library.
  *
  * Licensed to the Apache Software Foundation (ASF) under one or more
  * contributor license agreements.  See the NOTICE file distributed with
  * this work for additional information regarding copyright ownership.
  * The ASF licenses this file to You under the Apache License, Version 2.0
  * (the "License"); you may not use this file except in compliance with
  * the License.  You may obtain a copy of the License at
  *
  *    http://www.apache.org/licenses/LICENSE-2.0
  *
  * Unless required by applicable law or agreed to in writing, software
  * distributed under the License is distributed on an "AS IS" BASIS,
  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  * See the License for the specific language governing permissions and
  * limitations under the License.
  *
  */
object DateTimeUtils {

  // we use Int and Long internally to represent [[DateType]] and [[TimestampType]]
  type SQLDate = Int
  type SQLTimestamp = Long

  final val JULIAN_DAY_OF_EPOCH = 2440588
  final val SECONDS_PER_DAY = 60 * 60 * 24l
  final val MICROS_PER_MILLIS = 1000l
  final val MILLIS_PER_SECOND = 1000l
  final val MICROS_PER_SECOND = MICROS_PER_MILLIS * MILLIS_PER_SECOND
  final val MILLIS_PER_DAY = SECONDS_PER_DAY * 1000L

  /**
    * Returns the number of microseconds since epoch from Julian day
    * and nanoseconds in a day
    */
  def fromJulianDay(day: Int, nanoseconds: Long): SQLTimestamp = {
    // use Long to avoid rounding errors
    val seconds = (day - JULIAN_DAY_OF_EPOCH).toLong * SECONDS_PER_DAY
    seconds * MICROS_PER_SECOND + nanoseconds / 1000L
  }

  /*
   * Converts the timestamp to milliseconds since epoch. In spark timestamp values have microseconds
   * precision, so this conversion is lossy.
   */
  def toMillis(us: SQLTimestamp): Long = {
    // When the timestamp is negative i.e before 1970, we need to adjust the millseconds portion.
    // Example - 1965-01-01 10:11:12.123456 is represented as (-157700927876544) in micro precision.
    // In millis precision the above needs to be represented as (-157700927877).
    Math.floor(us.toDouble / MILLIS_PER_SECOND).toLong
  }

  def daysToMillis(days: SQLDate, timeZone: TimeZone): Long = {
    val millisLocal = days.toLong * MILLIS_PER_DAY
    millisLocal - timeZone.getRawOffset
  }

}
