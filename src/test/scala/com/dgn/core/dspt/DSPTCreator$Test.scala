package com.dgn.core.dspt

import com.dgn.core.utils.{DataFrameUtils, SparkUtils}
import com.dgn.testbase.{DGNBaseSparkTest, TestsPathsUtils}
import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.DataFrame

/**
  * Created by ophchu on 1/17/17.
  */
class DSPTCreator$Test extends DGNBaseSparkTest {

  test("testCreateDSPT") {
    DSPTCreator.createDSPT(dsptConfig)
    compareToParquet("spin", 127)
    compareToParquet("share", 17)
    compareToParquet("inbox", 18)
  }

//  test("convertToParquet"){
//    DSPTCreator.fetchKinesisEvents(
//      "2017-02-21",
//      "7",
//      TestsPathsUtils.getLtsInPath("kinesis/events"),
//      TestsPathsUtils.getLtsOutPath("kinesis/event"))
//
//    DSPTCreator.convertToParquet(
//    "spin",
//      TestsPathsUtils.getLtsOutPath("kinesis/event"),
//      TestsPathsUtils.getLtsOutPath("dspt/event"),
//      "2017-02-21",
//      "7"
//    )
//    compareToParquet("spin", 127)
//  }

  private def compareToParquet(eventType: String, size: Long) = {
    val schema = DataFrameUtils.createStructTypeFromFile(s"${TestsPathsUtils.getLtsInPath("schemas")}/$eventType.avsc")

    val origin = sps.read.schema(schema).json(TestsPathsUtils.getLtsOutPath(s"kinesis/event/eventType=$eventType/dt=2017-02-21/hr=7"))
    val target = sps.read.parquet(TestsPathsUtils.getLtsOutPath(s"dspt/event/eventType=$eventType/dt=2017-02-21/hr=7"))

    val ex = origin.except(target)

    origin.count shouldEqual size
    target.count shouldEqual size
    ex.count shouldEqual 0
  }

  test("fetchKinesisEvents"){
    DSPTCreator.fetchKinesisEvents(
      "2017-02-21",
      "7",
      TestsPathsUtils.getLtsInPath("kinesis/events"),
      TestsPathsUtils.getLtsOutPath("kinesis/event"), 4)
    checkEvents("spin", 127, 52)
    checkEvents("share", 17, 21)
  }

  private def checkEvents(eventType: String, count: Long, schemaSize: Int) = {
    val eventDF = sps.read.json(TestsPathsUtils.getLtsOutPath(s"kinesis/event/eventType=$eventType"))
    eventDF.count shouldEqual count
    eventDF.schema should have size schemaSize
  }



  private def compareDsptDspt(resultPath: String, expectedPath: String, expectedSize: Int) = {
    compareDFs(
      SparkUtils.readDF(resultPath),
      SparkUtils.readDF(expectedPath),
      expectedSize
    )
  }

  private def compareDsptRawDFs(resultPath: String, expectedPath: String, expectedSize: Int) = {
    compareDFs(
      SparkUtils.readDF(resultPath),
      SparkUtils.addDtHr(SparkUtils.readAvroFile(expectedPath)),
      expectedSize
    )
  }

  private def compareDFs(df1: DataFrame, df2: DataFrame, expectedSize: Int) = {
    df1.count shouldEqual df2.count
    df1.count shouldEqual expectedSize

    val (res1, res2) = DataFrameUtils.compareDF(df1, df2)

    res1.count shouldEqual 0
    res2.count shouldEqual 0
  }

  val confExample =
    s"""
      |dgn {
      |  dspt {
      |    dt = "2016-12-23"
      |    hr = 00
      |
      |    raw {
            in = ${TestsPathsUtils.getLtsInPath("raw/event/")}
      |    }
      |    dspt {
      |     out = ${TestsPathsUtils.getLtsOutPath("raw/event/")}
      |    }
      |
      |    events{
      |      in = [start_session, end_session]
      |    }
      |
      |  }
      |}
      |
    """.stripMargin


  val dsptConfig =
    s"""
      |dgn {
      |  dspt {
      |    dt = "2017-02-21"
      |    hr = 7
      |
      |    kinesis {
      |      in = ${TestsPathsUtils.getLtsInPath("kinesis/events")}
      |      out = ${TestsPathsUtils.getLtsOutPath("kinesis/event")}
      |    }
      |    events_wl=[spin,share,inbox,start_session]
      |    dspt {
      |      out = ${TestsPathsUtils.getLtsOutPath("/dspt/event/")}
      |      schema = ${TestsPathsUtils.getLtsInPath("schemas")}
      |    }
      |  }
      |}
      |
    """.stripMargin

}
