package com.dgn.core.utils

import com.dgn.testbase.{DGNBaseSparkTest, TestsPathsUtils}
import com.databricks.spark.avro._
/**
  * Created by ophchu on 1/12/17.
  */
class SparkAvroTest extends DGNBaseSparkTest {

  test("simple-avro-read"){
    val rawDF = sps.read.avro(TestsPathsUtils.getLtsInPath("raw/event/start_session/dt=2016-12-23/hr=00/lts_start_session.0.0.255.5166667.1482472800000.avro"))
    rawDF.count() shouldEqual 255
  }

}
