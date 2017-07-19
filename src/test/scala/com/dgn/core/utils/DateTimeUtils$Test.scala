package com.dgn.core.utils

import com.dgn.core.utils.DateTimeUtils.DTHR
import com.dgn.testbase.DGNBaseSparkTest

/**
  * Created by ophchu on 1/12/17.
  */
class DateTimeUtils$Test extends DGNBaseSparkTest {

  test("getLocalDateDaysRange"){
    val res = DateTimeUtils.getLocalDateDaysRange("2017-03-10", "2017-03-12")
    val res2 = DateTimeUtils.getLocalDateDaysRange("2017-03-12", "2017-03-10")
    val res3 = DateTimeUtils.getLocalDateDaysRange("2017-03-10", "2017-03-15")
    println()
  }

  test("getLocalDateDaysBack"){
    val res = DateTimeUtils.getLocalDateDaysRange("2017-03-10", 2)
    val res2 = DateTimeUtils.getLocalDateDaysRange("2017-03-10", -2)
    val res3 = DateTimeUtils.getLocalDateDaysRange("2017-03-10", -12)
  }

  test("testGetKinesisPath") {
    "/dgn-kinesis/events/2017/02/22/00" shouldEqual DateTimeUtils.formatKinesisPath("2017-02-22", "00", "/dgn-kinesis/events")
    "/dgn-kinesis/events/2017/01/23/00" shouldEqual DateTimeUtils.formatKinesisPath("2017-01-23", "00", "/dgn-kinesis/events")
    "/dgn-kinesis/events/2017/01/23/15" shouldEqual DateTimeUtils.formatKinesisPath("2017-01-23", "15", "/dgn-kinesis/events")
  }

  test("kinesisUTC") {
    DateTimeUtils.formatKinesisPath("2017-02-22", "7", "/dgn-kinesis/events") shouldEqual "/dgn-kinesis/events/2017/02/22/13"
    DateTimeUtils.formatKinesisPath("2017-02-22", "8", "/dgn-kinesis/events") shouldEqual "/dgn-kinesis/events/2017/02/22/14"
    DateTimeUtils.formatKinesisPath("2017-02-21", "22", "/dgn-kinesis/events") shouldEqual "/dgn-kinesis/events/2017/02/22/04"
  }

  test("formatKinesisPaths") {
    val res = DateTimeUtils.formatKinesisPaths("2017-02-22", "12", 0, 3, "/dgn-kinesis/events")
    res should have size 4
    res should contain ("/dgn-kinesis/events/2017/02/22/18")
    res should contain ("/dgn-kinesis/events/2017/02/22/19")
    res should contain ("/dgn-kinesis/events/2017/02/22/20")
    res should contain ("/dgn-kinesis/events/2017/02/22/21")
  }

  test("formatKinesisPaths2") {
    val res = DateTimeUtils.formatKinesisPaths("2017-02-22", "12", -2, 3, "/dgn-kinesis/events")
    res should have size 6
    res should contain ("/dgn-kinesis/events/2017/02/22/18")
    res should contain ("/dgn-kinesis/events/2017/02/22/19")
    res should contain ("/dgn-kinesis/events/2017/02/22/20")
    res should contain ("/dgn-kinesis/events/2017/02/22/21")


    res should contain ("/dgn-kinesis/events/2017/02/22/17")
    res should contain ("/dgn-kinesis/events/2017/02/22/16")
  }

  test("formatKinesisPaths3") {
    val res = DateTimeUtils.formatKinesisPaths("2017-02-22", "12", -1, 1, "/dgn-kinesis/events")
    res should have size 3
    res should contain ("/dgn-kinesis/events/2017/02/22/17")
    res should contain ("/dgn-kinesis/events/2017/02/22/18")
    res should contain ("/dgn-kinesis/events/2017/02/22/19")
  }




  test("testListDTHR") {
    DateTimeUtils.listDTHR(DTHR("2016-11-01", "00"), DTHR("2016-11-01", "10")) should have size 11
    DateTimeUtils.listDTHR(DTHR("2016-11-01", "00"), DTHR("2016-11-6", "23")) should have size (24 * 6)
    DateTimeUtils.listDTHR(DTHR("2016-11-01", "00"), DTHR("2016-11-31", "23")) should have size (24 * 31)
    DateTimeUtils.listDTHR(DTHR("2016-11-15", "00"), DTHR("2016-12-18", "23")) should have size (24 * 34)
  }

  test("testFilterDTHR") {
    DateTimeUtils
      .filterDTHR(DTHR("2016-11-01", "00"), DTHR("2016-11-01", "01")) shouldEqual
      """(dt="2016-11-01" and hr=00) or (dt="2016-11-01" and hr=01)"""
    DateTimeUtils
      .filterDTHR(DTHR("2016-11-01", "23"), DTHR("2016-11-02", "02")) shouldEqual
      """(dt="2016-11-01" and hr=23) or (dt="2016-11-02" and hr=00) or (dt="2016-11-02" and hr=01) or (dt="2016-11-02" and hr=02)"""
  }

  test("getHRFromTS"){
    DateTimeUtils.getHRFromTS(1487685239818L) shouldEqual 7
    DateTimeUtils.getHRFromTS(1487696682737L) shouldEqual 11
    DateTimeUtils.getHRFromTS(1487696682737L + (1000*60*60)) shouldEqual 12
  }

  test("getDTFromTS"){
    DateTimeUtils.getDTFromTS(1487685239818L) shouldEqual "2017-02-21"
    DateTimeUtils.getDTFromTS(1487696682737L) shouldEqual "2017-02-21"
    DateTimeUtils.getDTFromTS(1487696682737L + (1000*60*60)) shouldEqual "2017-02-21"
    DateTimeUtils.getDTFromTS(1487696682737L - (1000*60*60*12)) shouldEqual "2017-02-20"
  }

}
