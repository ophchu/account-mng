package com.dgn.core.derived.gamesession

import com.dgn.core.utils.{DataFrameUtils, SparkUtils}
import com.dgn.testbase.{DGNBaseSparkTest, TestsPathsUtils}
import org.apache.spark.sql.types.FloatType

/**
  * Created by ophchu on 1/18/17.
  */
class EventMethodsLTS$Test extends DGNBaseSparkTest {
  val gsOriginDF = SparkUtils.readDF(TestsPathsUtils.getLtsInPath("report/game_session/"))
    .where("""dt="2016-12-23" and hr=0""")

  test("testStartSpinEndEvent") {
    val gsDF = GameSessionCreator.createGameSessionLTS(baseConfig.format("start_session,spin,end_session"))
    gsDF.count() shouldEqual 3064

    val fields = gsDF.schema.map(_.name)

    fields should contain allOf (EventMethodsLTS.startSessionCols(0), EventMethodsLTS.startSessionCols(1), EventMethodsLTS.startSessionCols.tail.tail: _*)
//    fields should contain (EventMethodsLTS.addSpinCols)
//    fields should contain (EventMethodsLTS.endSessionCols)
  }

  test("testAddStartSessionEvent2") {
    val gsDF = GameSessionCreator.createGameSessionLTS(baseConfig.format("start_session"))
    val selectedGsDF = gsOriginDF.selectExpr(EventMethodsLTS.startSessionCols: _*)

    val exCheck  = DataFrameUtils.compareDF2(gsDF.na.fill(0), selectedGsDF.na.fill(0))
    exCheck.leftRegDF.count() shouldEqual 0
    exCheck.rightRegDF.count() shouldEqual 3
    gsDF.count() shouldEqual 3064
  }

  test("testAddSpinEvent") {
    val gsDF = GameSessionCreator.createGameSessionLTS(baseConfig.format("spin"))
      .selectExpr(EventMethodsLTS.addSpinCols: _*)
    val selectedGsDF = gsOriginDF.selectExpr(EventMethodsLTS.addSpinCols: _*)

    val exCheck  = DataFrameUtils.compareDF3(gsDF.na.fill(0).drop("free_spin_count"), selectedGsDF.drop("free_spin_count"))
    exCheck.leftRegDF.count() shouldEqual 12
    exCheck.rightRegDF.count() shouldEqual 3

    exCheck.longMapsDF.get._1.count() shouldEqual 3
    exCheck.longMapsDF.get._2.count() shouldEqual 3

    exCheck.intMapsDF shouldBe None

    gsDF.count() shouldEqual 3076
  }

  test("testEndSessionEvent") {
    val gsDF = GameSessionCreator.createGameSessionLTS(baseConfig.format("start_session,end_session"))
      .selectExpr(EventMethodsLTS.endSessionCols: _*)
    val selectedGsDF = gsOriginDF.selectExpr(EventMethodsLTS.endSessionCols: _*)

    val exCheck  = DataFrameUtils.compareDF2(gsDF.na.fill(0), selectedGsDF)
    exCheck.leftRegDF.count() shouldEqual 0
    exCheck.rightRegDF.count() shouldEqual 3
    gsDF.count() shouldEqual 3064
  }

  test("testCollectBonusEvent2") {
    val gsDF = GameSessionCreator.createGameSessionLTS(baseConfigDtHR.format("2017-05-24", 8, "collect_bonus"))
      .selectExpr(EventMethodsLTS.addCollectBonusCols: _*)
    val collectBonusDF = gsDF.select("collect_bonus") .filter(
      """session_id in
        |("prod_6009bfc9e44d49538512ed3da5937295",
        |"prod_59783b43cc7d46f198194e179888cfad")""".stripMargin).select("collect_bonus")

    collectBonusDF.count() shouldEqual 2
  }

  test("testCollectBonusEvent") {
    val gsDF = GameSessionCreator.createGameSessionLTS(baseConfig.format("collect_bonus"))
      .selectExpr(EventMethodsLTS.addCollectBonusCols: _*)
    val selectedGsDF = gsOriginDF.selectExpr(EventMethodsLTS.addCollectBonusCols: _*)

    val exCheck  = DataFrameUtils.compareDF3(gsDF.na.fill(0), selectedGsDF)

    exCheck.leftRegDF.count() shouldEqual 12
    exCheck.rightRegDF.count() shouldEqual 3

    exCheck.longMapsDF.get._1.count() shouldEqual 1
    exCheck.longMapsDF.get._2.count() shouldEqual 2

    exCheck.intMapsDF shouldBe None

    gsDF.count() shouldEqual 3076
  }

  test("testOpenSlotEvent") {
    val gsDF = GameSessionCreator.createGameSessionLTS(baseConfig.format("open_slot"))
      .selectExpr(EventMethodsLTS.addOpenSlotCols: _*)
    val selectedGsDF = gsOriginDF.selectExpr(EventMethodsLTS.addOpenSlotCols: _*)
      .where("open_slot_count > 0")

    val exCheck  = DataFrameUtils.compareDF3(gsDF.na.fill(0), selectedGsDF)
    exCheck.leftRegDF.count() shouldEqual 1
    exCheck.rightRegDF.count() shouldEqual 3

    exCheck.longMapsDF.get._1.count() shouldEqual 1
    exCheck.longMapsDF.get._2.count() shouldEqual 3

    exCheck.intMapsDF shouldBe None

    gsDF.count() shouldEqual 3076
  }

  test("testTransactionEvent") {
    val gsDF = GameSessionCreator.createGameSessionLTS(baseConfig.format("start_session,transaction"))
      .selectExpr(EventMethodsLTS.addTransactionCols: _*)
    val selectedGsDF = gsOriginDF.selectExpr(EventMethodsLTS.addTransactionCols: _*)

    val typeMap = gsDF.dtypes.map(column =>
      column._2 match {
        case "IntegerType" => column._1 -> 0
        case "LongType" => column._1 -> 0
        case "StringType" => column._1 -> ""
        case "DoubleType" => column._1 -> 0.0
        case "FloatType" => column._1 -> 0.0
        case "BooleanType" => column._1 -> false
      }).toMap

    val exCheck  = DataFrameUtils.compareDF3(gsDF.na.fill(typeMap), selectedGsDF.na.fill(typeMap))

    exCheck.leftRegDF.count() shouldEqual 9
    exCheck.rightRegDF.count() shouldEqual 0

//    exCheck.longMapsDF.get._1.count() shouldEqual 1
//    exCheck.longMapsDF.get._2.count() shouldEqual 3

    exCheck.intMapsDF shouldBe None
    exCheck.longMapsDF shouldBe None

    gsDF.count() shouldEqual 3076
  }

  test("testCashierEvent") {
    val gsDF = GameSessionCreator.createGameSessionLTS(baseConfig.format("start_session,cashier"))
      .selectExpr(EventMethodsLTS.addCashierCols: _*)
    val selectedGsDF = gsOriginDF.selectExpr(EventMethodsLTS.addCashierCols: _*)

    val exCheck  = DataFrameUtils.compareDF2(gsDF.na.fill(0), selectedGsDF)
    exCheck.leftRegDF.count() shouldEqual 0
    exCheck.rightRegDF.count() shouldEqual 3
    gsDF.count() shouldEqual 3064
  }

  test("testFacebookConnectEvent") {
    val gsDF = GameSessionCreator.createGameSessionLTS(baseConfig.format("start_session,facebook_connect"))
      .selectExpr(EventMethodsLTS.addFacebookConnectCols: _*)
    val selectedGsDF = gsOriginDF.selectExpr(EventMethodsLTS.addFacebookConnectCols: _*)

    val exCheck  = DataFrameUtils.compareDF2(gsDF.na.fill(0), selectedGsDF)
    exCheck.leftRegDF.count() shouldEqual 0
    exCheck.rightRegDF.count() shouldEqual 3
    gsDF.count() shouldEqual 3064
  }

  test("testBackToLobbyEvent") {
    val gsDF = GameSessionCreator.createGameSessionLTS(baseConfig.format("start_session,back_to_lobby"))
      .selectExpr(EventMethodsLTS.addBackToLobbyCols: _*)
    val selectedGsDF = gsOriginDF.selectExpr(EventMethodsLTS.addBackToLobbyCols: _*)

    val exCheck  = DataFrameUtils.compareDF3(gsDF.na.fill(0), selectedGsDF)
    exCheck.leftRegDF.count() shouldEqual 0
    exCheck.rightRegDF.count() shouldEqual 3

    exCheck.longMapsDF.get._1.count() shouldEqual 0
    exCheck.longMapsDF.get._2.count() shouldEqual 1

    exCheck.intMapsDF shouldBe None

    gsDF.count() shouldEqual 3064
  }

  test("testBonusGameEvent") {
    val gsDF = GameSessionCreator.createGameSessionLTS(baseConfig.format("start_session,bonus_game"))
      .selectExpr(EventMethodsLTS.addBonusGameCols: _*)
    val selectedGsDF = gsOriginDF.selectExpr(EventMethodsLTS.addBonusGameCols: _*)

    val exCheck  = DataFrameUtils.compareDF2(gsDF.na.fill(0), selectedGsDF)
    exCheck.leftRegDF.count() shouldEqual 0
    exCheck.rightRegDF.count() shouldEqual 3

    exCheck.leftMapDF.count() shouldEqual 0
    exCheck.rightMapDF.count() shouldEqual 0
    gsDF.count() shouldEqual 3064
  }

  test("testFacebookDisconnectEvent") {
    val gsDF = GameSessionCreator.createGameSessionLTS(baseConfig.format("facebook_disconnect"))
      .selectExpr(EventMethodsLTS.addFacebookDisconnectCols: _*)
    val selectedGsDF = gsOriginDF.selectExpr(EventMethodsLTS.addFacebookDisconnectCols: _*)

    val exCheck  = DataFrameUtils.compareDF2(gsDF.na.fill(0), selectedGsDF)
    exCheck.leftRegDF.count() shouldEqual 0
    exCheck.rightRegDF.count() shouldEqual 3
    gsDF.count() shouldEqual 3064
  }

  test("testInboxEvent") {
    val gsDF = GameSessionCreator.createGameSessionLTS(inboxExample)
      .selectExpr(EventMethodsLTS.addInboxCols: _*)
    val selectedGsDF = gsOriginDF.selectExpr(EventMethodsLTS.addInboxCols: _*)

    val exCheck  = DataFrameUtils.compareDF2(gsDF.na.fill(0), selectedGsDF)
    exCheck.leftRegDF.count() shouldEqual 0
    exCheck.rightRegDF.count() shouldEqual 3
    gsDF.count() shouldEqual 3064
  }

  test("testInviteEvent") {
    val gsDF = GameSessionCreator.createGameSessionLTS(inviteExample)
      .selectExpr(EventMethodsLTS.addInviteCols: _*)
    val selectedGsDF = gsOriginDF.selectExpr(EventMethodsLTS.addInviteCols: _*)

    val exCheck  = DataFrameUtils.compareDF2(gsDF.na.fill(0), selectedGsDF)
    exCheck.leftRegDF.count() shouldEqual 0
    exCheck.rightRegDF.count() shouldEqual 3
    gsDF.count() shouldEqual 3064
  }

  test("testMissionEvent") {
    val gsDF = GameSessionCreator.createGameSessionLTS(missionExample)
      .selectExpr(EventMethodsLTS.addMissionCols: _*)
    val selectedGsDF = gsOriginDF.selectExpr(EventMethodsLTS.addMissionCols: _*)

    val exCheck  = DataFrameUtils.compareDF2(gsDF.na.fill(0), selectedGsDF)
    exCheck.leftRegDF.count() shouldEqual 0
    exCheck.rightRegDF.count() shouldEqual 3

    exCheck.leftMapDF.count() shouldEqual 0
    exCheck.rightMapDF.count() shouldEqual 0
    gsDF.count() shouldEqual 3064
  }

  test("testRecommendationEvent") {
    val gsDF = GameSessionCreator.createGameSessionLTS(recommendationExample)
      .selectExpr(EventMethodsLTS.addRecommendationCols: _*)
    val selectedGsDF = gsOriginDF.selectExpr(EventMethodsLTS.addRecommendationCols: _*)

    val exCheck  = DataFrameUtils.compareDF2(gsDF.na.fill(0), selectedGsDF)
    exCheck.leftRegDF.count() shouldEqual 3
    exCheck.rightRegDF.count() shouldEqual 6

    exCheck.leftMapDF.count() shouldEqual 0
    exCheck.rightMapDF.count() shouldEqual 0
    gsDF.count() shouldEqual 3064
  }

  test("testRequestGiftEvent") {
    val gsDF = GameSessionCreator.createGameSessionLTS(requestGiftExample)
      .selectExpr(EventMethodsLTS.addRequestGiftCols: _*)
    val selectedGsDF = gsOriginDF.selectExpr(EventMethodsLTS.addRequestGiftCols: _*)

    val exCheck  = DataFrameUtils.compareDF2(gsDF.na.fill(0), selectedGsDF)
    exCheck.leftRegDF.count() shouldEqual 0
    exCheck.rightRegDF.count() shouldEqual 3
    gsDF.count() shouldEqual 3064
  }

  test("testSendGiftEvent") {
    val gsDF = GameSessionCreator.createGameSessionLTS(sendGiftExample)
      .selectExpr(EventMethodsLTS.addSendGiftCols: _*)
    val selectedGsDF = gsOriginDF.selectExpr(EventMethodsLTS.addSendGiftCols: _*)

    val exCheck  = DataFrameUtils.compareDF2(gsDF.na.fill(0), selectedGsDF)
    exCheck.leftRegDF.count() shouldEqual 0
    exCheck.rightRegDF.count() shouldEqual 3
    gsDF.count() shouldEqual 3064
  }

  test("shareEvent") {
    val gsDF = GameSessionCreator.createGameSessionLTS(shareExample)
      .selectExpr(EventMethodsLTS.addShareCols: _*)
    val selectedGsDF = gsOriginDF.selectExpr(EventMethodsLTS.addShareCols: _*)

    val exCheck  = DataFrameUtils.compareDF2(gsDF.na.fill(0), selectedGsDF)
    exCheck.leftRegDF.count() shouldEqual 0
    exCheck.rightRegDF.count() shouldEqual 3
    gsDF.count() shouldEqual 3064
  }

  test("testStopEvent") {
    val gsDF = GameSessionCreator.createGameSessionLTS(stopExample)
      .selectExpr(EventMethodsLTS.addStopCols: _*)
    val selectedGsDF = gsOriginDF.selectExpr(EventMethodsLTS.addStopCols: _*)

    val exCheck  = DataFrameUtils.compareDF2(gsDF.na.fill(0), selectedGsDF)
    exCheck.leftRegDF.count() shouldEqual 0
    exCheck.rightRegDF.count() shouldEqual 3

    exCheck.leftMapDF.count() shouldEqual 0
    exCheck.rightMapDF.count() shouldEqual 0
    gsDF.count() shouldEqual 3064
  }

  val baseConfigDtHR =
    s"""
       |dgn {
       |  gs {
       |    dt = "%s"
       |    hr = %d
       |    hours_back = 3
       |    join_on = "session_id"
       |    rm_map_nulls = [spin, stop, collect_bonus, open_slot,stop, bonus_game, mission, recommendation, back_to_lobby]
       |    event_prefix = ""
       |    schema = ${TestsPathsUtils.getLtsInPath("schemas")}
       |    game_session = {
       |      in = ${TestsPathsUtils.getLtsInPath("event/")}
       |      out = ${TestsPathsUtils.getLtsOutPath("report/game_session")}
       |    }
       |    events = [%s]
       |  }
       |}
     """.stripMargin

  val baseConfig =
    s"""
       |dgn {
       |  gs {
       |    dt = "2016-12-23"
       |    hr = 00
       |    hours_back = 3
       |    join_on = "session_id"
       |    rm_map_nulls = [spin, stop, collect_bonus, open_slot,stop, bonus_game, mission, recommendation, back_to_lobby]
       |    event_prefix = ""
       |    parts = 4
       |    schema = ${TestsPathsUtils.getLtsInPath("schemas")}
       |    game_session = {
       |      in = ${TestsPathsUtils.getLtsInPath("event/")}
       |      out = ${TestsPathsUtils.getLtsOutPath("report/game_session")}
       |    }
       |    events = [%s]
       |  }
       |}
     """.stripMargin

  val startSessionExample =
    s"""
       |dgn {
       |  gs {
       |    dt = "2016-12-23"
       |    hr = 00
       |    hours_back = 3
       |    join_on = "session_id"
       |    event_prefix = ""
       |    game_session = {
       |      in = ${TestsPathsUtils.getLtsInPath("event/")}
       |      out = ${TestsPathsUtils.getLtsOutPath("report/game_session")}
       |    }
       |    events = ["start_session"]
       |  }
       |}
     """.stripMargin

  val addSpinExample =
    s"""
       |dgn {
       |  gs {
       |    dt = "2016-12-23"
       |    hr = 00
       |    hours_back = 3
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

  val endSessionExample =
    s"""
       |dgn {
       |  gs {
       |    dt = "2016-12-23"
       |    hr = 00
       |    hours_back = 3
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
       |        name = start_session
       |        in = ${TestsPathsUtils.getLtsInPath("event/start_session")}
       |      }
       |      {
       |        name = end_session
       |        in = ${TestsPathsUtils.getLtsInPath("event/end_session")}
       |      }
       |    ]
       |  }
       |}
     """.stripMargin

  val collectBonusExample =
    s"""
       |dgn {
       |  gs {
       |    dt = "2016-12-23"
       |    hr = 00
       |    hours_back = 3
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
       |        name = start_session
       |        in = ${TestsPathsUtils.getLtsInPath("event/start_session")}
       |      }
       |      {
       |        name = collect_bonus
       |        in = ${TestsPathsUtils.getLtsInPath("event/collect_bonus")}
       |      }
       |    ]
       |  }
       |}
     """.stripMargin

  val transactionExample =
    s"""
       |dgn {
       |  gs {
       |    dt = "2016-12-23"
       |    hr = 00
       |    hours_back = 3
       |    join_on = "session_id"
       |    game_session = {
       |      out = ${TestsPathsUtils.getLtsOutPath("report/game_session")}
       |    }
       |    end_session = {
       |        in = ${TestsPathsUtils.getLtsInPath("event/end_session")}
       |    }
       |    events = [
       |      {
       |        name = start_session
       |        in = ${TestsPathsUtils.getLtsInPath("event/start_session")}
       |      }
       |      {
       |        name = transaction
       |        in = ${TestsPathsUtils.getLtsInPath("event/transaction")}
       |      }
       |    ]
       |  }
       |}
     """.stripMargin

  val cashierExample =
    s"""
       |dgn {
       |  gs {
       |    dt = "2016-12-23"
       |    hr = 00
       |    hours_back = 3
       |    join_on = "session_id"
       |    game_session = {
       |      out = ${TestsPathsUtils.getLtsOutPath("report/game_session")}
       |    }
       |    end_session = {
       |        in = ${TestsPathsUtils.getLtsInPath("event/end_session")}
       |    }
       |    events = [
       |      {
       |        name = start_session
       |        in = ${TestsPathsUtils.getLtsInPath("event/start_session")}
       |      }
       |      {
       |        name = cashier
       |        in = ${TestsPathsUtils.getLtsInPath("event/cashier")}
       |      }
       |    ]
       |  }
       |}
     """.stripMargin

  val facebookConnectExample =
    s"""
       |dgn {
       |  gs {
       |    dt = "2016-12-23"
       |    hr = 00
       |    hours_back = 3
       |    join_on = "session_id"
       |    game_session = {
       |      out = ${TestsPathsUtils.getLtsOutPath("report/game_session")}
       |    }
       |    end_session = {
       |        in = ${TestsPathsUtils.getLtsInPath("event/end_session")}
       |    }
       |    events = [
       |      {
       |        name = start_session
       |        in = ${TestsPathsUtils.getLtsInPath("event/start_session")}
       |      }
       |      {
       |        name = facebook_connect
       |        in = ${TestsPathsUtils.getLtsInPath("event/facebook_connect")}
       |      }
       |    ]
       |  }
       |}
     """.stripMargin

  val facebookDisconnectExample =
    s"""
       |dgn {
       |  gs {
       |    dt = "2016-12-23"
       |    hr = 00
       |    hours_back = 3
       |    join_on = "session_id"
       |    game_session = {
       |      out = ${TestsPathsUtils.getLtsOutPath("report/game_session")}
       |    }
       |    end_session = {
       |        in = ${TestsPathsUtils.getLtsInPath("event/end_session")}
       |    }
       |    events = [
       |      {
       |        name = start_session
       |        in = ${TestsPathsUtils.getLtsInPath("event/start_session")}
       |      }
       |      {
       |        name = facebook_disconnect
       |        in = ${TestsPathsUtils.getLtsInPath("event/facebook_disconnect")}
       |      }
       |    ]
       |  }
       |}
     """.stripMargin

  val inboxExample =
    s"""
       |dgn {
       |  gs {
       |    dt = "2016-12-23"
       |    hr = 00
       |    hours_back = 3
       |    join_on = "session_id"
       |    game_session = {
       |      out = ${TestsPathsUtils.getLtsOutPath("report/game_session")}
       |    }
       |    end_session = {
       |        in = ${TestsPathsUtils.getLtsInPath("event/end_session")}
       |    }
       |    events = [
       |      {
       |        name = start_session
       |        in = ${TestsPathsUtils.getLtsInPath("event/start_session")}
       |      }
       |      {
       |        name = inbox
       |        in = ${TestsPathsUtils.getLtsInPath("event/inbox")}
       |      }
       |    ]
       |  }
       |}
     """.stripMargin

  val inviteExample =
    s"""
       |dgn {
       |  gs {
       |    dt = "2016-12-23"
       |    hr = 00
       |    hours_back = 3
       |    join_on = "session_id"
       |    game_session = {
       |      out = ${TestsPathsUtils.getLtsOutPath("report/game_session")}
       |    }
       |    end_session = {
       |        in = ${TestsPathsUtils.getLtsInPath("event/end_session")}
       |    }
       |    events = [
       |      {
       |        name = start_session
       |        in = ${TestsPathsUtils.getLtsInPath("event/start_session")}
       |      }
       |      {
       |        name = invite
       |        in = ${TestsPathsUtils.getLtsInPath("event/invite")}
       |      }
       |    ]
       |  }
       |}
     """.stripMargin

  val requestGiftExample =
    s"""
       |dgn {
       |  gs {
       |    dt = "2016-12-23"
       |    hr = 00
       |    hours_back = 3
       |    join_on = "session_id"
       |    game_session = {
       |      out = ${TestsPathsUtils.getLtsOutPath("report/game_session")}
       |    }
       |    end_session = {
       |        in = ${TestsPathsUtils.getLtsInPath("event/end_session")}
       |    }
       |    events = [
       |      {
       |        name = start_session
       |        in = ${TestsPathsUtils.getLtsInPath("event/start_session")}
       |      }
       |      {
       |        name = request_gift
       |        in = ${TestsPathsUtils.getLtsInPath("event/request_gift")}
       |      }
       |    ]
       |  }
       |}
     """.stripMargin

  val sendGiftExample =
    s"""
       |dgn {
       |  gs {
       |    dt = "2016-12-23"
       |    hr = 00
       |    hours_back = 3
       |    join_on = "session_id"
       |    game_session = {
       |      out = ${TestsPathsUtils.getLtsOutPath("report/game_session")}
       |    }
       |    end_session = {
       |        in = ${TestsPathsUtils.getLtsInPath("event/end_session")}
       |    }
       |    events = [
       |      {
       |        name = start_session
       |        in = ${TestsPathsUtils.getLtsInPath("event/start_session")}
       |      }
       |      {
       |        name = send_gift
       |        in = ${TestsPathsUtils.getLtsInPath("event/send_gift")}
       |      }
       |    ]
       |  }
       |}
     """.stripMargin

  val shareExample =
    s"""
       |dgn {
       |  gs {
       |    dt = "2016-12-23"
       |    hr = 00
       |    hours_back = 3
       |    join_on = "session_id"
       |    game_session = {
       |      out = ${TestsPathsUtils.getLtsOutPath("report/game_session")}
       |    }
       |    end_session = {
       |        in = ${TestsPathsUtils.getLtsInPath("event/end_session")}
       |    }
       |    events = [
       |      {
       |        name = start_session
       |        in = ${TestsPathsUtils.getLtsInPath("event/start_session")}
       |      }
       |      {
       |        name = share
       |        in = ${TestsPathsUtils.getLtsInPath("event/share")}
       |      }
       |    ]
       |  }
       |}
     """.stripMargin

  val missionExample =
    s"""
       |dgn {
       |  gs {
       |    dt = "2016-12-23"
       |    hr = 00
       |    hours_back = 3
       |    join_on = "session_id"
       |    game_session = {
       |      out = ${TestsPathsUtils.getLtsOutPath("report/game_session")}
       |    }
       |    end_session = {
       |        in = ${TestsPathsUtils.getLtsInPath("event/end_session")}
       |    }
       |    events = [
       |      {
       |        name = start_session
       |        in = ${TestsPathsUtils.getLtsInPath("event/start_session")}
       |      }
       |      {
       |        name = mission
       |        in = ${TestsPathsUtils.getLtsInPath("event/mission")}
       |      }
       |    ]
       |  }
       |}
     """.stripMargin

  val recommendationExample =
    s"""
       |dgn {
       |  gs {
       |    dt = "2016-12-23"
       |    hr = 00
       |    hours_back = 3
       |    join_on = "session_id"
       |    game_session = {
       |      out = ${TestsPathsUtils.getLtsOutPath("report/game_session")}
       |    }
       |    end_session = {
       |        in = ${TestsPathsUtils.getLtsInPath("event/end_session")}
       |    }
       |    events = [
       |      {
       |        name = start_session
       |        in = ${TestsPathsUtils.getLtsInPath("event/start_session")}
       |      }
       |      {
       |        name = recommendation
       |        in = ${TestsPathsUtils.getLtsInPath("event/recommendation")}
       |      }
       |    ]
       |  }
       |}
     """.stripMargin

  val stopExample =
    s"""
       |dgn {
       |  gs {
       |    dt = "2016-12-23"
       |    hr = 00
       |    hours_back = 3
       |    join_on = "session_id"
       |    game_session = {
       |      out = ${TestsPathsUtils.getLtsOutPath("report/game_session")}
       |    }
       |    end_session = {
       |        in = ${TestsPathsUtils.getLtsInPath("event/end_session")}
       |    }
       |    events = [
       |      {
       |        name = start_session
       |        in = ${TestsPathsUtils.getLtsInPath("event/start_session")}
       |      }
       |      {
       |        name = stop
       |        in = ${TestsPathsUtils.getLtsInPath("event/stop")}
       |      }
       |    ]
       |  }
       |}
     """.stripMargin

  val bonusGameExample =
    s"""
       |dgn {
       |  gs {
       |    dt = "2016-12-23"
       |    hr = 00
       |    hours_back = 3
       |    join_on = "session_id"
       |    game_session = {
       |      out = ${TestsPathsUtils.getLtsOutPath("report/game_session")}
       |    }
       |    end_session = {
       |        in = ${TestsPathsUtils.getLtsInPath("event/end_session")}
       |    }
       |    events = [
       |      {
       |        name = start_session
       |        in = ${TestsPathsUtils.getLtsInPath("event/start_session")}
       |      }
       |      {
       |        name = bonus_game
       |        in = ${TestsPathsUtils.getLtsInPath("event/bonus_game")}
       |      }
       |    ]
       |  }
       |}
     """.stripMargin

  val backToLobbyExample =
    s"""
       |dgn {
       |  gs {
       |    dt = "2016-12-23"
       |    hr = 00
       |    hours_back = 3
       |    join_on = "session_id"
       |    game_session = {
       |      out = ${TestsPathsUtils.getLtsOutPath("report/game_session")}
       |    }
       |    end_session = {
       |        in = ${TestsPathsUtils.getLtsInPath("event/end_session")}
       |    }
       |    events = [
       |      {
       |        name = start_session
       |        in = ${TestsPathsUtils.getLtsInPath("event/start_session")}
       |      }
       |      {
       |        name = back_to_lobby
       |        in = ${TestsPathsUtils.getLtsInPath("event/back_to_lobby")}
       |      }
       |    ]
       |  }
       |}
     """.stripMargin

  val openSlotExample =
    s"""
       |dgn {
       |  gs {
       |    dt = "2016-12-23"
       |    hr = 00
       |    hours_back = 3
       |    join_on = "session_id"
       |    game_session = {
       |      out = ${TestsPathsUtils.getLtsOutPath("report/game_session")}
       |    }
       |    end_session = {
       |        in = ${TestsPathsUtils.getLtsInPath("event/end_session")}
       |    }
       |    events = [
       |      {
       |        name = start_session
       |        in = ${TestsPathsUtils.getLtsInPath("event/start_session")}
       |      }
       |      {
       |        name = open_slot
       |        in = ${TestsPathsUtils.getLtsInPath("event/open_slot")}
       |      }
       |    ]
       |  }
       |}
     """.stripMargin
}

