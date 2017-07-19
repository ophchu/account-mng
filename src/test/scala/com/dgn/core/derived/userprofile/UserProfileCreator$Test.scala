package com.dgn.core.derived.userprofile

import com.dgn.core.utils.SparkUtils
import com.dgn.testbase.{DGNBaseSparkTest, TestsPathsUtils}

/**
  * Created by ophchu on 1/12/17.
  */
class UserProfileCreator$Test extends DGNBaseSparkTest {

  ignore("testCreateUserProfile") {
    val userProfile = UserProfileCreator.createUserProfileLTS(confExample)

    val userKpiDF = SparkUtils.readDF(TestsPathsUtils.getLtsInPath("report/user_kpi/")).where("""dt="2016-12-23"""")
//    userProfile.count shouldEqual userKpiDF.count
    userProfile.count shouldEqual 50609
  }
  val confExample =
    s"""
      |dgn {
      |  up {
      |    dt = "2016-12-23"
      |    days_back = 2
      |
      |    game_session_daily = {
      |      in = ${TestsPathsUtils.getLtsInPath("report/game_session_daily")}
      |    }
      |    user_kpi = {
      |      out = ${TestsPathsUtils.getLtsOutPath("report/user_kpi")}
      |    }
      |    user_kpi_decile = {
      |      out = ${TestsPathsUtils.getLtsOutPath("report/user_kpi_decile")}
      |    }
      |    user_profile = {
      |      out = ${TestsPathsUtils.getLtsOutPath("report/user_profile")}
      |    }
      |
      |  }
      |}
      |
    """.stripMargin

}
