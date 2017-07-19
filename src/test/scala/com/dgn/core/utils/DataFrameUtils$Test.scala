package com.dgn.core.utils

import com.dgn.core.derived.gamesession.{EventMethodsLTS, GameSessionCreator}
import com.dgn.testbase.{DGNBaseSparkTest, TestsPathsUtils}
import org.scalatest.FunSuite

/**
  * Created by ophchu on 1/29/17.
  */
class DataFrameUtils$Test extends DGNBaseSparkTest {
  val gsOriginDF = SparkUtils.readDF(TestsPathsUtils.getLtsInPath("report/game_session/")).where("""dt="2016-12-23" and hr=0""")


  ignore("testCompareDFMaps") {
      val gsDF = GameSessionCreator.createGameSessionLTS(addSpinExample).selectExpr("session_id", "spin")
      val selectedGsDF = gsOriginDF.selectExpr("session_id", "spin")

      val (ex, res) = DataFrameUtils.compareDF(gsDF, selectedGsDF)
      ex.count() shouldEqual 3
      res.count() shouldEqual 1563
      gsDF.count() shouldEqual 1507
  }

  val addSpinExample =
    s"""
       |dgn {
       |  gs {
       |    dt = "2016-12-23"
       |    hr = 00
       |    hours_back = 0
       |    join_on = "session_id"
       |    rm_map_nulls = [spin, collect_bonus, open_slot,stop, bonus_game, mission, recommendation, back_to_lobby]
       |    game_session = {
       |      out = ${TestsPathsUtils.getLtsOutPath("report/game_session")}
       |    }
       |    end_session = {
       |        in = ${TestsPathsUtils.getLtsInPath("event/end_session")}
       |    }
       |    events = [
       |      {
       |        name = spin
       |        in = ${TestsPathsUtils.getLtsInPath("event/spin")}
       |      }
       |    ]
       |  }
       |}
     """.stripMargin
}
