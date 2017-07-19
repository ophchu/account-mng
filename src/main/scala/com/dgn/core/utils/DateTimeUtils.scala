package com.dgn.core.utils

import java.time._
import java.time.format.DateTimeFormatter
import java.util.TimeZone

/**
  * Created by ophchu on 1/12/17.
  */
object DateTimeUtils {
  val defaultTimeZone = "America/Chicago"
  val kinesisPathFormatter = DateTimeFormatter.ofPattern("yyyy/MM/dd/HH")
  val defaultZoneId = ZoneId.of(defaultTimeZone)
  val utcZoneId = ZoneId.of("UTC")

  def getCurrentDateTime(minusHour: Int = 0) = {
    LocalDateTime.now(defaultZoneId).minusHours(minusHour)
  }

  def getCurrentDate = {
    LocalDate.now(defaultZoneId)
  }

  def getLocalDateTimeHoursBack(dt: String, hr: String, minusHours: Int): List[ZonedDateTime] = {
    val localDateTime = getLocalDateTime(dt, hr.toInt)
    (0 to minusHours).map(minusHour => localDateTime.minusHours(minusHour)).toList
  }

  @deprecated
  def getLocalDateDaysBack(dt: String, minusDays: Int): List[LocalDate] = {
    val localDate = getLocalDate(dt)
    val min = Math.min(0, minusDays)
    val max = Math.max(0, minusDays)
    (min to max).map(minusDay => localDate.minusDays(minusDay)).toList
  }



  def getLocalDateDaysRange(dt: String, plusDays: Int): List[LocalDate] = {
    val localDate = getLocalDate(dt)
    (Math.min(0, plusDays) to Math.max(0, plusDays))
      .map(plus => localDate.plusDays(plus)).toList
  }

  def getLocalDateDaysRange(dtStart: String, dtEnd: String): List[LocalDate] = {
    val start = getLocalDate(dtStart)
    val end = getLocalDate(dtEnd)

    def genDtRange(from: LocalDate, to: LocalDate): List[LocalDate] = {
      from.isEqual(to) match {
        case true => List(from)
        case false => from :: genDtRange(from.plusDays(1), to)
      }
    }
    if (start.isAfter(end)){
      List.empty[LocalDate]
    }else{
      genDtRange(start, end)
    }
  }

  def getLocalDateTime(dt: String, hr: String): ZonedDateTime = {
    getLocalDateTime(dt, hr.toInt)
  }

  def getLocalDateTime(dt: String, hr: Int): ZonedDateTime = {
    LocalDateTime.of(LocalDate.parse(dt), LocalTime.of(hr, 0, 0)).atZone(defaultZoneId)
  }

  def getLocalDate(dt: String): LocalDate = {
    LocalDate.parse(dt)
  }

  def formatKinesisPaths(dt: String, hr: String, hoursFrom: Int, hoursTo: Int, kinesisRoot: String) = {
    val localDateTime = getLocalDateTime(dt, hr)
    (hoursFrom to hoursTo).map(hour => formatKinesisPath(localDateTime.plusHours(hour), kinesisRoot)).toList
  }

  def formatKinesisPath(dt: String, hr: String, kinesisRoot: String): String = {
    formatKinesisPath(getLocalDateTime(dt, hr).withZoneSameInstant(utcZoneId), kinesisRoot)
  }

  def formatKinesisPath(localDateTime: ZonedDateTime, kinesisRoot: String): String = {
    s"$kinesisRoot/${localDateTime.withZoneSameInstant(utcZoneId).format(kinesisPathFormatter)}"
  }

  def getHRFromTS(ts: Long) = {
    getZonedTimeDate(ts).getHour
  }

  def getDTFromTS(ts: Long) = {
    getZonedTimeDate(ts).toLocalDate.toString
  }

  def getZonedTimeDate(ts: Long) = {
    ZonedDateTime.ofInstant(Instant.ofEpochMilli(ts), defaultZoneId);
  }
  //*******************************************************************************************
  //Below should be deprecated
  //*******************************************************************************************

  val HOUR_IN_MILLIS = 1000 * 60 * 60
  val dtHrFormater = new java.text.SimpleDateFormat("yyyy-MM-dd-HH")
  val dtFormater = new java.text.SimpleDateFormat("yyyy-MM-dd")
  val hrFormater = new java.text.SimpleDateFormat("HH")

  @deprecated
  case class DTHR(dt: String, hr: String)

  val hrChickagoFormater = new java.text.SimpleDateFormat("HH")
  hrChickagoFormater.setTimeZone(TimeZone.getTimeZone(defaultTimeZone))
  val dtChickagoFormater = new java.text.SimpleDateFormat("yyyy-MM-dd")
  dtChickagoFormater.setTimeZone(TimeZone.getTimeZone(defaultTimeZone))


  @deprecated("Use getHRFromTS")
  def getTzHR(ts: Long) = hrChickagoFormater.format(ts).toInt

  @deprecated("Use getDTFromTS")
  def getTzDT(ts: Long) = dtChickagoFormater.format(ts)


  @deprecated
  def filterDTHR(currentDT: String, currentHR: String, hoursBack: Int): String = {
    val currentTime = dtHrFormater.parse(s"$currentDT-$currentHR").getTime
    val startTime = currentTime - (hoursBack * HOUR_IN_MILLIS)
    filterDTHR(
      DTHR(dtFormater.format(startTime), hrFormater.format(startTime)),
      DTHR(dtFormater.format(currentTime), hrFormater.format(currentTime)))
  }

  def listDTHR(currentDT: String, currentHR: String, hoursBack: Int): List[DTHR] = {
    val currentTime = dtHrFormater.parse(s"$currentDT-$currentHR").getTime
    val startTime = currentTime - (hoursBack * HOUR_IN_MILLIS)
    listDTHR(
      DTHR(dtFormater.format(startTime), hrFormater.format(startTime)),
      DTHR(dtFormater.format(currentTime), hrFormater.format(currentTime)))
  }.toList

  def filterDTHR(start: DTHR, end: DTHR) = {
    createDTHRFilter(listDTHR(start, end))
  }

  def listDTHR(start: DTHR, end: DTHR) = {
    val startTime = dtHrFormater.parse(s"${start.dt}-${start.hr}")
    val endTime = dtHrFormater.parse(s"${end.dt}-${end.hr}")

    (Math.min(startTime.getTime, endTime.getTime) to Math.max(startTime.getTime, endTime.getTime) by HOUR_IN_MILLIS) map {
      hour =>
        DTHR(dtFormater.format(hour), hrFormater.format(hour))
    }
  }

  def createDTHRFilter(dthrList: Seq[DTHR]) = {
    dthrList.map(dthr => s"""(dt="${dthr.dt}" and hr=${dthr.hr})""").mkString(" or ")
  }
}
