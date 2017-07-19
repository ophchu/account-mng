package com.dgn.core.utils

import com.dgn.testbase.{DGNBaseSparkTest, TestsPathsUtils}

/**
  * Created by ophchu on 3/8/17.
  */
class SparkUtils$Test extends DGNBaseSparkTest {

  test("testReadDF - two hours") {
    val df2220 = sps.read.option("basePath", TestsPathsUtils.getLtsInPath("event/start_session/")).parquet(TestsPathsUtils.getLtsInPath("event/start_session/dt=2016-12-22/hr=20"))
    val df2221 = sps.read.option("basePath", TestsPathsUtils.getLtsInPath("event/start_session/")).parquet(TestsPathsUtils.getLtsInPath("event/start_session/dt=2016-12-22/hr=21"))
    val dfAtOnce = SparkUtils.readDF(TestsPathsUtils.getLtsInPath("event/start_session/dt=2016-12-22/"), "hr=20", "hr=21")

    df2220.count shouldEqual 3848
    df2221.count shouldEqual 3633
    dfAtOnce.count shouldEqual 7481

    val res1 = dfAtOnce.except(df2220)
    val res2 = res1.except(df2221)

    res2.count() shouldEqual 0

  }

  test("testReadDF - no subdirs") {
    val df22 = sps.read.option("basePath", TestsPathsUtils.getLtsInPath("event/start_session/")).parquet(TestsPathsUtils.getLtsInPath("event/start_session/dt=2016-12-22/"))
    val dfAtOnce = SparkUtils.readDF(TestsPathsUtils.getLtsInPath("event/start_session/dt=2016-12-22/"))

    df22.count shouldEqual 14290
    dfAtOnce.count shouldEqual 14290

    val res1 = dfAtOnce.except(df22)

    res1.count() shouldEqual 0
  }

}
