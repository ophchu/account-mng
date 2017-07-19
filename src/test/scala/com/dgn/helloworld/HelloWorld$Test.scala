package com.dgn.helloworld

import com.dgn.testbase.DGNBaseSparkTest
import org.apache.spark.sql.SaveMode
import org.json4s._
import org.json4s.jackson.JsonMethods._

import scala.util.Random


/**
  * Created by ophchu on 12/28/16.
  */
class HelloWorld$Test extends DGNBaseSparkTest {

  ignore("reverse hello world") {
    val res = HelloWorld.reverseHelloWorld shouldEqual "HelloWorld!".reverse
  }

  ignore("read json") {
    sps.sparkContext.hadoopConfiguration.set("textinputformat.record.delimiter","**##**")
    println(sps.sparkContext.hadoopConfiguration.get("textinputformat.record.delimiter")  )
    val ds = sps.sparkContext.textFile("/home/ophchu/slg/repos/dgn-spark-dbc/src/test/resources/in/lts/kinesis/events/2017/02/21/13")


//    ds.write.partitionBy()
    val ds2 = sps.read.json("/home/ophchu/slg/test-json-2")
    println()
  }
//  test("read rdd") {
//    case class RddTest(eventType: String, dt: String, value: String)
//
//    import sps.implicits._
//    sps.sparkContext.hadoopConfiguration.set("textinputformat.record.delimiter","**##**")
//    val rdd = sps.sparkContext.textFile("/home/ophchu/slg/test-json2").filter(_.length>1)
//
//
//    val res = rdd.map(line => {
//      val jsonMap = parse(line).values.asInstanceOf[Map[String, Any]]
//      RddTest(jsonMap("event_type").toString, rndDT, line)
//    }
//    ).toDS()
//    res.write.mode(SaveMode.Overwrite).partitionBy("eventType").text("/home/ophchu/slg/test-json-out2")
//
//  }
  val rnd = Random
  val dtList = List("2017-02-14", "2017-02-15")
  def rndDT = {
    dtList(rnd.nextInt(2))
  }

  test("test jackson") {

  }
}
