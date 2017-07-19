package com.dgn.core.playground

import com.dgn.core.utils.SparkUtils
import org.apache.avro.specific.SpecificDatumReader
import org.apache.commons.io.FileUtils
import org.apache.spark.sql._

/**
  * Created by ophchu on 2/12/17.
  */
object AvroPlayground {

  def readStartSession(path: String)
                      (implicit sps: SparkSession) = {
    val startSessionDF = SparkUtils.readDF("/home/ophchu/slg/repos/dgn-spark-dbc/src/test/resources/in/lts/event/start_session/dt=2016-12-23/hr=00")


  }

}
