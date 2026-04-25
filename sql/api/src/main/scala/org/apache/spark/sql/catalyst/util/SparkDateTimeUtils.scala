/*
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
 */
package org.apache.spark.sql.catalyst.util

import java.sql.{Date, Timestamp}
import java.time.{Instant, LocalDate, LocalDateTime, LocalTime, ZonedDateTime, ZoneId, ZoneOffset}
import java.util.TimeZone
import java.util.concurrent.TimeUnit.{MICROSECONDS, NANOSECONDS}

import scala.util.control.NonFatal

import sun.util.calendar.ZoneInfo

import org.apache.spark.sql.catalyst.trees.SQLQueryContext
import org.apache.spark.sql.catalyst.util.DateTimeConstants._
import org.apache.spark.sql.catalyst.util.RebaseDateTime.{rebaseGregorianToJulianDays, rebaseGregorianToJulianMicros, rebaseJulianToGregorianDays, rebaseJulianToGregorianMicros}
import org.apache.spark.sql.errors.ExecutionErrors
import org.apache.spark.sql.types.{DateType, TimestampType}
import org.apache.spark.unsafe.types.UTF8String

trait SparkDateTimeUtils {

  final val TimeZoneUTC = TimeZone.getTimeZone("UTC")

  def getZoneId(timeZoneId: String): ZoneId = {
    val formattedZoneId = timeZoneId
      // To support the (+|-)h:mm format because it was supported before Spark 3.0.
      .replaceFirst("(\\+|\\-)(\\d):", "$10$2:")
      // To support the (+|-)hh:m format because it was supported before Spark 3.0.
      .replaceFirst("(\\+|\\-)(\\d\\d):(\\d)$", "$1$2:0$3")

    ZoneId.of(formattedZoneId, ZoneId.SHORT_IDS)
  }

  def getTimeZone(timeZoneId: String): TimeZone = TimeZone.getTimeZone(getZoneId(timeZoneId))

  /**
   * Converts an Java object to days.
   *
   * @param obj Either an object of `java.sql.Date` or `java.time.LocalDate`.
   * @return The number of days since 1970-01-01.
   */
  def anyToDays(obj: Any): Int = obj match {
    case d: Date => fromJavaDate(d)
    case ld: LocalDate => localDateToDays(ld)
  }

  /**
   * Converts an Java object to microseconds.
   *
   * @param obj Either an object of `java.sql.Timestamp` or `java.time.{Instant,LocalDateTime}`.
   * @return The number of micros since the epoch.
   */
  def anyToMicros(obj: Any): Long = obj match {
    case t: Timestamp => fromJavaTimestamp(t)
    case i: Instant => instantToMicros(i)
    case ldt: LocalDateTime => localDateTimeToMicros(ldt)
  }

  /**
   * Converts the timestamp to milliseconds since epoch. In Spark timestamp values have microseconds
   * precision, so this conversion is lossy.
   */
  def microsToMillis(micros: Long): Long = {
    // When the timestamp is negative i.e before 1970, we need to adjust the milliseconds portion.
    // Example - 1965-01-01 10:11:12.123456 is represented as (-157700927876544) in micro precision.
    // In millis precision the above needs to be represented as (-157700927877).
    Math.floorDiv(micros, MICROS_PER_MILLIS)
  }

  /**
   * Converts milliseconds since the epoch to microseconds.
   */
  def millisToMicros(millis: Long): Long = {
    Math.multiplyExact(millis, MICROS_PER_MILLIS)
  }

  // See issue SPARK-35679
  // min second cause overflow in instant to micro
  private val MIN_SECONDS = Math.floorDiv(Long.MinValue, MICROS_PER_SECOND)

  /**
   * Obtains an instance of `java.time.Instant` using microseconds from
   * the epoch of 1970-01-01 00:00:00Z.
   */
  def microsToInstant(micros: Long): Instant = {
    val secs = Math.floorDiv(micros, MICROS_PER_SECOND)
    // Unfolded Math.floorMod(us, MICROS_PER_SECOND) to reuse the result of
    // the above calculation of `secs` via `floorDiv`.
    val mos = micros - secs * MICROS_PER_SECOND
    Instant.ofEpochSecond(secs, mos * NANOS_PER_MICROS)
  }

  /**
   * Gets the number of microseconds since the epoch of 1970-01-01 00:00:00Z from the given
   * instance of `java.time.Instant`. The epoch microsecond count is a simple incrementing count of
   * microseconds where microsecond 0 is 1970-01-01 00:00:00Z.
   */
  def instantToMicros(instant: Instant): Long = {
    val secs = instant.getEpochSecond
    if (secs == MIN_SECONDS) {
      val us = Math.multiplyExact(secs + 1, MICROS_PER_SECOND)
      Math.addExact(us, NANOSECONDS.toMicros(instant.getNano) - MICROS_PER_SECOND)
    } else {
      val us = Math.multiplyExact(secs, MICROS_PER_SECOND)
      Math.addExact(us, NANOSECONDS.toMicros(instant.getNano))
    }
  }

  /**
   * Converts the timestamp `micros` from one timezone to another.
   *
   * Time-zone rules, such as daylight savings, mean that not every local date-time
   * is valid for the `toZone` time zone, thus the local date-time may be adjusted.
   */
  def convertTz(micros: Long, fromZone: ZoneId, toZone: ZoneId): Long = {
    val rebasedDateTime = getLocalDateTime(micros, toZone).atZone(fromZone)
    instantToMicros(rebasedDateTime.toInstant)
  }

  // Gets the local date-time parts (year, month, day and time) of the instant expressed as the
  // number of microseconds since the epoch at the given time zone ID.
  protected def getLocalDateTime(micros: Long, zoneId: ZoneId): LocalDateTime = {
    microsToInstant(micros).atZone(zoneId).toLocalDateTime
  }

  def microsToLocalDateTime(micros: Long): LocalDateTime = {
    getLocalDateTime(micros, ZoneOffset.UTC)
  }

  def localDateTimeToMicros(localDateTime: LocalDateTime): Long = {
    instantToMicros(localDateTime.toInstant(ZoneOffset.UTC))
  }

  /**
   * Converts the local date to the number of days since 1970-01-01.
   */
  def localDateToDays(localDate: LocalDate): Int = MathUtils.toIntExact(localDate.toEpochDay)

  /**
   * Obtains an instance of `java.time.LocalDate` from the epoch day count.
   */
  def daysToLocalDate(days: Int): LocalDate = LocalDate.ofEpochDay(days)

  /**
   * Converts microseconds since 1970-01-01 00:00:00Z to days since 1970-01-01 at the given zone ID.
   */
  def microsToDays(micros: Long, zoneId: ZoneId): Int = {
    localDateToDays(getLocalDateTime(micros, zoneId).toLocalDate)
  }

  /**
   * Converts days since 1970-01-01 at the given zone ID to microseconds since 1970-01-01 00:00:00Z.
   */
  def daysToMicros(days: Int, zoneId: ZoneId): Long = {
    val instant = daysToLocalDate(days).atStartOfDay(zoneId).toInstant
    instantToMicros(instant)
  }

  /**
   * Converts a local date at the default JVM time zone to the number of days since 1970-01-01
   * in the hybrid calendar (Julian + Gregorian) by discarding the time part. The resulted days are
   * rebased from the hybrid to Proleptic Gregorian calendar. The days rebasing is performed via
   * UTC time zone for simplicity because the difference between two calendars is the same in
   * any given time zone and UTC time zone.
   *
   * Note: The date is shifted by the offset of the default JVM time zone for backward compatibility
   *       with Spark 2.4 and earlier versions. The goal of the shift is to get a local date derived
   *       from the number of days that has the same date fields (year, month, day) as the original
   *       `date` at the default JVM time zone.
   *
   * @param date It represents a specific instant in time based on the hybrid calendar which
   *             combines Julian and Gregorian calendars.
   * @return The number of days since the epoch in Proleptic Gregorian calendar.
   */
  def fromJavaDate(date: Date): Int = {
    val millisUtc = date.getTime
    val millisLocal = millisUtc + TimeZone.getDefault.getOffset(millisUtc)
    val julianDays = Math.toIntExact(Math.floorDiv(millisLocal, MILLIS_PER_DAY))
    rebaseJulianToGregorianDays(julianDays)
  }

  /**
   * Converts days since the epoch 1970-01-01 in Proleptic Gregorian calendar to a local date
   * at the default JVM time zone in the hybrid calendar (Julian + Gregorian). It rebases the given
   * days from Proleptic Gregorian to the hybrid calendar at UTC time zone for simplicity because
   * the difference between two calendars doesn't depend on any time zone. The result is shifted
   * by the time zone offset in wall clock to have the same date fields (year, month, day)
   * at the default JVM time zone as the input `daysSinceEpoch` in Proleptic Gregorian calendar.
   *
   * Note: The date is shifted by the offset of the default JVM time zone for backward compatibility
   *       with Spark 2.4 and earlier versions.
   *
   * @param days The number of days since 1970-01-01 in Proleptic Gregorian calendar.
   * @return A local date in the hybrid calendar as `java.sql.Date` from number of days since epoch.
   */
  def toJavaDate(days: Int): Date = {
    val rebasedDays = rebaseGregorianToJulianDays(days)
    val localMillis = Math.multiplyExact(rebasedDays, MILLIS_PER_DAY)
    val timeZoneOffset = TimeZone.getDefault match {
      case zoneInfo: ZoneInfo => zoneInfo.getOffsetsByWall(localMillis, null)
      case timeZone: TimeZone => timeZone.getOffset(localMillis - timeZone.getRawOffset)
    }
    new Date(localMillis - timeZoneOffset)
  }

  /**
   * Converts microseconds since the epoch to an instance of `java.sql.Timestamp`
   * via creating a local timestamp at the system time zone in Proleptic Gregorian
   * calendar, extracting date and time fields like `year` and `hours`, and forming
   * new timestamp in the hybrid calendar from the extracted fields.
   *
   * The conversion is based on the JVM system time zone because the `java.sql.Timestamp`
   * uses the time zone internally.
   *
   * The method performs the conversion via local timestamp fields to have the same date-time
   * representation as `year`, `month`, `day`, ..., `seconds` in the original calendar
   * and in the target calendar.
   *
   * @param micros The number of microseconds since 1970-01-01T00:00:00.000000Z.
   * @return A `java.sql.Timestamp` from number of micros since epoch.
   */
  def toJavaTimestamp(micros: Long): Timestamp =
    toJavaTimestampNoRebase(rebaseGregorianToJulianMicros(micros))

  def toJavaTimestamp(timeZoneId: String, micros: Long): Timestamp =
    toJavaTimestampNoRebase(rebaseGregorianToJulianMicros(timeZoneId, micros))

  /**
   * Converts microseconds since the epoch to an instance of `java.sql.Timestamp`.
   *
   * @param micros The number of microseconds since 1970-01-01T00:00:00.000000Z.
   * @return A `java.sql.Timestamp` from number of micros since epoch.
   */
  def toJavaTimestampNoRebase(micros: Long): Timestamp = {
    val seconds = Math.floorDiv(micros, MICROS_PER_SECOND)
    val ts = new Timestamp(seconds * MILLIS_PER_SECOND)
    val nanos = (micros - seconds * MICROS_PER_SECOND) * NANOS_PER_MICROS
    ts.setNanos(nanos.toInt)
    ts
  }

  /**
   * Converts an instance of `java.sql.Timestamp` to the number of microseconds since
   * 1970-01-01T00:00:00.000000Z. It extracts date-time fields from the input, builds
   * a local timestamp in Proleptic Gregorian calendar from the fields, and binds
   * the timestamp to the system time zone. The resulted instant is converted to
   * microseconds since the epoch.
   *
   * The conversion is performed via the system time zone because it is used internally
   * in `java.sql.Timestamp` while extracting date-time fields.
   *
   * The goal of the function is to have the same local date-time in the original calendar
   * - the hybrid calendar (Julian + Gregorian) and in the target calendar which is
   * Proleptic Gregorian calendar, see SPARK-26651.
   *
   * @param t It represents a specific instant in time based on
   *          the hybrid calendar which combines Julian and
   *          Gregorian calendars.
   * @return The number of micros since epoch from `java.sql.Timestamp`.
   */
  def fromJavaTimestamp(t: Timestamp): Long =
    rebaseJulianToGregorianMicros(fromJavaTimestampNoRebase(t))

  def fromJavaTimestamp(timeZoneId: String, t: Timestamp): Long =
    rebaseJulianToGregorianMicros(timeZoneId, fromJavaTimestampNoRebase(t))

  /**
   * Converts an instance of `java.sql.Timestamp` to the number of microseconds since
   * 1970-01-01T00:00:00.000000Z.
   *
   * @param t an instance of `java.sql.Timestamp`.
   * @return The number of micros since epoch from `java.sql.Timestamp`.
   */
  def fromJavaTimestampNoRebase(t: Timestamp): Long =
    millisToMicros(t.getTime) + (t.getNanos / NANOS_PER_MICROS) % MICROS_PER_MILLIS

  /**
   * Trims and parses a given UTF8 date string to a corresponding [[Int]] value.
   * The return type is [[Option]] in order to distinguish between 0 and null. The following
   * formats are allowed:
   *
   * `[+-]yyyy*`
   * `[+-]yyyy*-[m]m`
   * `[+-]yyyy*-[m]m-[d]d`
   * `[+-]yyyy*-[m]m-[d]d `
   * `[+-]yyyy*-[m]m-[d]d *`
   * `[+-]yyyy*-[m]m-[d]dT*`
   */
  // 将 UTF8 字符串解析为日期整数（即距离 1970-01-01 的天数）的工具函数。
  // 它不依赖于重量级的标准库解析器，而是通过直接操作字节数组来实现高性能解析。
  // 代码开头的注释和解析逻辑明确了它支持以下几种组合：
  //
  //全日期格式：yyyy-MM-dd（如 2023-10-27）
  //
  //缩减格式：
  //
  //yyyy-MM（如 2023-10，此时“日”默认为 1）
  //
  //yyyy（如 2023，此时“月”和“日”都默认为 1）
  //
  //带时间前缀的格式：
  //
  //yyyy-MM-dd HH:mm:ss...（空格分隔，函数会解析日期部分并忽略空格后的时间）
  //
  //yyyy-MM-ddTHH:mm:ss...（T 分隔，符合 ISO 标准，同样会忽略 T 之后的时间）
  // 数值范围要求
  //年份 (Year)：
  //
  //支持正负号（+ 或 -），意味着可以处理公元前（BC）的日期。
  //
  //位数限制为 4 到 7 位。这意味着它支持从 0001 年到 9999999 年（约 500 万年以内）。
  //
  //月份 (Month)：
  //
  //必须是 1 到 2 位 数字（如 1 或 01）。
  //
  //必须在 1-12 之间（由底层的 LocalDate.of 校验）。
  //
  //日期 (Day)：
  //
  //必须是 1 到 2 位 数字。
  //
  //必须符合该月的实际天数（如 2023-02-29 会因为是非闰年而在 try-catch 中解析失败）。
  def stringToDate(s: UTF8String): Option[Int] = {
    // 内部辅助函数：验证当前时间段（年、月、日）的数字位数是否合法
    // segment == 0 代表“年”，Spark 支持 [+-]500万年，所以位数在 4 到 7 位之间
    // segment != 0 代表“月”或“日”，必须是 1 到 2 位数字
    def isValidDigits(segment: Int, digits: Int): Boolean = {
      // An integer is able to represent a date within [+-]5 million years.
      val maxDigitsYear = 7
      (segment == 0 && digits >= 4 && digits <= maxDigitsYear) ||
        (segment != 0 && digits > 0 && digits <= 2)
    }

    // 如果输入为空或仅包含空格，返回 None
    if (s == null || s.trimAll().numBytes() == 0) {
      return None
    }
    // 存储解析出的 [年, 月, 日]，默认月日为 1
    val segments: Array[Int] = Array[Int](1, 1, 1)
    var sign = 1 // 符号位，正数或负数年份
    var i = 0   // 当前正在解析哪个段（0:年, 1:月, 2:日）
    var currentSegmentValue = 0  // 当前段的数值累计
    var currentSegmentDigits = 0 // 当前段已读取的数字个数
    val bytes = s.trimAll().getBytes // 获取修剪空格后的字节数组
    var j = 0 // 字节数组的指针
    if (bytes(j) == '-' || bytes(j) == '+') {
      sign = if (bytes(j) == '-') -1 else 1  // 处理开头的正负号
      j += 1
    }
    // 循环条件：未读完、段数少于3、且没遇到空格或 'T'（ISO时间分隔符）
    while (j < bytes.length && (i < 3 && !(bytes(j) == ' ' || bytes(j) == 'T'))) {
      val b = bytes(j)
      if (i < 2 && b == '-') { // 如果遇到连接符 '-' 且还没到“日”段
        if (!isValidDigits(i, currentSegmentDigits)) { // 位数不合法（如 202-1-1）则解析失败
          return None
        }
        segments(i) = currentSegmentValue
        currentSegmentValue = 0
        currentSegmentDigits = 0
        i += 1
      } else {
        val parsedValue = b - '0'.toByte
        if (parsedValue < 0 || parsedValue > 9) {
          return None
        } else {
          currentSegmentValue = currentSegmentValue * 10 + parsedValue
          currentSegmentDigits += 1
        }
      }
      j += 1
    }
    if (!isValidDigits(i, currentSegmentDigits)) {
      return None
    }
    // 边界检查：如果是 yyyy 或 yyyy-mm 格式，后面不应该还有剩余字符（除了空格/T之后的内容）
    if (i < 2 && j < bytes.length) {
      // For the `yyyy` and `yyyy-[m]m` formats, entire input must be consumed.
      return None
    }
    segments(i) = currentSegmentValue
    try {
      // 使用 Java 8 的 LocalDate 构建日期对象
      // segments(0) 是年，segments(1) 是月，segments(2) 是日
      val localDate = LocalDate.of(sign * segments(0), segments(1), segments(2))
      // 调用辅助方法将 LocalDate 转换为从 1970-01-01 开始计算的整数天数
      Some(localDateToDays(localDate))
    } catch {
      case NonFatal(_) => None
    }
  }

  def stringToDateAnsi(
      s: UTF8String,
      context: SQLQueryContext = null): Int = {
    stringToDate(s).getOrElse {
      throw ExecutionErrors.invalidInputInCastToDatetimeError(s, DateType, context)
    }
  }

  /**
   * Trims and parses a given UTF8 timestamp string to the corresponding timestamp segments,
   * time zone id and whether it is just time without a date.
   * value. The return type is [[Option]] in order to distinguish between 0L and null. The following
   * formats are allowed:
   *
   * `[+-]yyyy*`
   * `[+-]yyyy*-[m]m`
   * `[+-]yyyy*-[m]m-[d]d`
   * `[+-]yyyy*-[m]m-[d]d `
   * `[+-]yyyy*-[m]m-[d]d [h]h:[m]m:[s]s.[ms][ms][ms][us][us][us][zone_id]`
   * `[+-]yyyy*-[m]m-[d]dT[h]h:[m]m:[s]s.[ms][ms][ms][us][us][us][zone_id]`
   * `[h]h:[m]m:[s]s.[ms][ms][ms][us][us][us][zone_id]`
   * `T[h]h:[m]m:[s]s.[ms][ms][ms][us][us][us][zone_id]`
   *
   * where `zone_id` should have one of the forms:
   *   - Z - Zulu time zone UTC+0
   *   - +|-[h]h:[m]m
   *   - A short id, see https://docs.oracle.com/javase/8/docs/api/java/time/ZoneId.html#SHORT_IDS
   *   - An id with one of the prefixes UTC+, UTC-, GMT+, GMT-, UT+ or UT-,
   *     and a suffix in the formats:
   *     - +|-h[h]
   *     - +|-hh[:]mm
   *     - +|-hh:mm:ss
   *     - +|-hhmmss
   *  - Region-based zone IDs in the form `area/city`, such as `Europe/Paris`
   *
   * @return timestamp segments, time zone id and whether the input is just time without a date. If
   *         the input string can't be parsed as timestamp, the result timestamp segments are empty.
   */
  def parseTimestampString(s: UTF8String): (Array[Int], Option[ZoneId], Boolean) = {
    def isValidDigits(segment: Int, digits: Int): Boolean = {
      // A Long is able to represent a timestamp within [+-]200 thousand years
      val maxDigitsYear = 6
      // For the nanosecond part, more than 6 digits is allowed, but will be truncated.
      segment == 6 || (segment == 0 && digits >= 4 && digits <= maxDigitsYear) ||
        // For the zoneId segment(7), it's could be zero digits when it's a region-based zone ID
        (segment == 7 && digits <= 2) ||
        (segment != 0 && segment != 6 && segment != 7 && digits > 0 && digits <= 2)
    }
    if (s == null || s.trimAll().numBytes() == 0) {
      return (Array.empty, None, false)
    }
    var tz: Option[String] = None
    val segments: Array[Int] = Array[Int](1, 1, 1, 0, 0, 0, 0, 0, 0)
    var i = 0
    var currentSegmentValue = 0
    var currentSegmentDigits = 0
    val bytes = s.trimAll().getBytes
    var j = 0
    var digitsMilli = 0
    var justTime = false
    var yearSign: Option[Int] = None
    if (bytes(j) == '-' || bytes(j) == '+') {
      yearSign = if (bytes(j) == '-') Some(-1) else Some(1)
      j += 1
    }
    while (j < bytes.length) {
      val b = bytes(j)
      val parsedValue = b - '0'.toByte
      if (parsedValue < 0 || parsedValue > 9) {
        if (j == 0 && b == 'T') {
          justTime = true
          i += 3
        } else if (i < 2) {
          if (b == '-') {
            if (!isValidDigits(i, currentSegmentDigits)) {
              return (Array.empty, None, false)
            }
            segments(i) = currentSegmentValue
            currentSegmentValue = 0
            currentSegmentDigits = 0
            i += 1
          } else if (i == 0 && b == ':' && yearSign.isEmpty) {
            justTime = true
            if (!isValidDigits(3, currentSegmentDigits)) {
              return (Array.empty, None, false)
            }
            segments(3) = currentSegmentValue
            currentSegmentValue = 0
            currentSegmentDigits = 0
            i = 4
          } else {
            return (Array.empty, None, false)
          }
        } else if (i == 2) {
          if (b == ' ' || b == 'T') {
            if (!isValidDigits(i, currentSegmentDigits)) {
              return (Array.empty, None, false)
            }
            segments(i) = currentSegmentValue
            currentSegmentValue = 0
            currentSegmentDigits = 0
            i += 1
          } else {
            return (Array.empty, None, false)
          }
        } else if (i == 3 || i == 4) {
          if (b == ':') {
            if (!isValidDigits(i, currentSegmentDigits)) {
              return (Array.empty, None, false)
            }
            segments(i) = currentSegmentValue
            currentSegmentValue = 0
            currentSegmentDigits = 0
            i += 1
          } else {
            return (Array.empty, None, false)
          }
        } else if (i == 5 || i == 6) {
          if (b == '.' && i == 5) {
            if (!isValidDigits(i, currentSegmentDigits)) {
              return (Array.empty, None, false)
            }
            segments(i) = currentSegmentValue
            currentSegmentValue = 0
            currentSegmentDigits = 0
            i += 1
          } else {
            if (!isValidDigits(i, currentSegmentDigits)) {
              return (Array.empty, None, false)
            }
            segments(i) = currentSegmentValue
            currentSegmentValue = 0
            currentSegmentDigits = 0
            i += 1
            tz = Some(new String(bytes, j, bytes.length - j))
            j = bytes.length - 1
          }
          if (i == 6  && b != '.') {
            i += 1
          }
        } else {
          if (i < segments.length && (b == ':' || b == ' ')) {
            if (!isValidDigits(i, currentSegmentDigits)) {
              return (Array.empty, None, false)
            }
            segments(i) = currentSegmentValue
            currentSegmentValue = 0
            currentSegmentDigits = 0
            i += 1
          } else {
            return (Array.empty, None, false)
          }
        }
      } else {
        if (i == 6) {
          digitsMilli += 1
        }
        // We will truncate the nanosecond part if there are more than 6 digits, which results
        // in loss of precision
        if (i != 6 || currentSegmentDigits < 6) {
          currentSegmentValue = currentSegmentValue * 10 + parsedValue
        }
        currentSegmentDigits += 1
      }
      j += 1
    }

    if (!isValidDigits(i, currentSegmentDigits)) {
      return (Array.empty, None, false)
    }
    segments(i) = currentSegmentValue

    while (digitsMilli < 6) {
      segments(6) *= 10
      digitsMilli += 1
    }

    // This step also validates time zone part
    val zoneId = tz.map(zoneName => getZoneId(zoneName.trim))
    segments(0) *= yearSign.getOrElse(1)
    (segments, zoneId, justTime)
  }

  /**
   * Trims and parses a given UTF8 timestamp string to the corresponding a corresponding [[Long]]
   * value. The return type is [[Option]] in order to distinguish between 0L and null. Please
   * refer to `parseTimestampString` for the allowed formats
   */
  // 将 UTF8 字符串解析为时间戳（Timestamp） 的核心逻辑
  def stringToTimestamp(s: UTF8String, timeZoneId: ZoneId): Option[Long] = {
    try {
      val (segments, parsedZoneId, justTime) = parseTimestampString(s)
      if (segments.isEmpty) {
        return None
      }
      val zoneId = parsedZoneId.getOrElse(timeZoneId)
      val nanoseconds = MICROSECONDS.toNanos(segments(6))
      val localTime = LocalTime.of(segments(3), segments(4), segments(5), nanoseconds.toInt)
      val localDate = if (justTime) {
        LocalDate.now(zoneId)
      } else {
        LocalDate.of(segments(0), segments(1), segments(2))
      }
      val localDateTime = LocalDateTime.of(localDate, localTime)
      val zonedDateTime = ZonedDateTime.of(localDateTime, zoneId)
      val instant = Instant.from(zonedDateTime)
      Some(instantToMicros(instant))
    } catch {
      case NonFatal(_) => None
    }
  }

  def stringToTimestampAnsi(
      s: UTF8String,
      timeZoneId: ZoneId,
      context: SQLQueryContext = null): Long = {
    stringToTimestamp(s, timeZoneId).getOrElse {
      throw ExecutionErrors.invalidInputInCastToDatetimeError(s, TimestampType, context)
    }
  }

  /**
   * Trims and parses a given UTF8 string to a corresponding [[Long]] value which representing the
   * number of microseconds since the epoch. The result will be independent of time zones.
   *
   * If the input string contains a component associated with time zone, the method will return
   * `None` if `allowTimeZone` is set to `false`. If `allowTimeZone` is set to `true`, the method
   * will simply discard the time zone component. Enable the check to detect situations like parsing
   * a timestamp with time zone as TimestampNTZType.
   *
   * The return type is [[Option]] in order to distinguish between 0L and null. Please
   * refer to `parseTimestampString` for the allowed formats.
   */
  def stringToTimestampWithoutTimeZone(s: UTF8String, allowTimeZone: Boolean): Option[Long] = {
    try {
      val (segments, zoneIdOpt, justTime) = parseTimestampString(s)
      // If the input string can't be parsed as a timestamp without time zone, or it contains only
      // the time part of a timestamp and we can't determine its date, return None.
      if (segments.isEmpty || justTime || !allowTimeZone && zoneIdOpt.isDefined) {
        return None
      }
      val nanoseconds = MICROSECONDS.toNanos(segments(6))
      val localTime = LocalTime.of(segments(3), segments(4), segments(5), nanoseconds.toInt)
      val localDate = LocalDate.of(segments(0), segments(1), segments(2))
      val localDateTime = LocalDateTime.of(localDate, localTime)
      Some(localDateTimeToMicros(localDateTime))
    } catch {
      case NonFatal(_) => None
    }
  }
}

object SparkDateTimeUtils extends SparkDateTimeUtils
