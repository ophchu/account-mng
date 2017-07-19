package com.dgn.core.utils

import com.dgn.testbase.DGNBaseTest
import org.scalatest.FunSuite

/**
  * Created by ophchu on 5/9/17.
  */
class CloudWatchUtils$Test extends DGNBaseTest {

  test("sendMetrics"){
    CloudWatchUtils.sendJobTime("testit_time", 50)
  }
  test("sendJobStatus"){
    CloudWatchUtils.sendJobStatus("testit_status", true)
  }

  test("sendJobTime"){
    List(2000, 1000, 2500, 2700, 1200, 1300).foreach(time => {
      sendTime("job_test_time_2", time)
      Thread.sleep(200)
    })
  }
  test("this"){
    val gsSchema = DataFrameUtils.createStructTypeFromFile("/home/ophchu/slg/repos/dgn-spark-dbc/src/main/resources/avro/lts/origin/agg/game_session.avsc")
    println()
  }
  private def sendTime(jobName: String, time: Long) = {
    CloudWatchUtils.startMeasureTime(jobName)
    Thread.sleep(time)
    CloudWatchUtils.endAndSendMeasure(jobName)
  }
}
