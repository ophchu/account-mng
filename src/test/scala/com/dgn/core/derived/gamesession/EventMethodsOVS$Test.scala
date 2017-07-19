package com.dgn.core.derived.gamesession

import com.dgn.core.utils.{DataFrameUtils, SparkUtils}
import com.dgn.testbase.{DGNBaseSparkTest, TestsPathsUtils}
import org.apache.spark.sql.types.FloatType

/**
  * Created by tamirm on 5/23/17.
  */
class EventMethodsOVS$Test extends DGNBaseSparkTest {
  val gsOriginDF = SparkUtils.readDF(TestsPathsUtils.getOvsInPath("report/game_session/"))
    .where("""dt="2017-05-01" and hr=10""")

  test("testStartSpinEndEvent") {
    val gsDF = GameSessionCreator.createGameSessionOVS(baseConfig.format("start_session,spin,end_session"))
    gsDF.count() shouldEqual 11825

    val fields = gsDF.schema.map(_.name)
    fields should contain allOf (EventMethodsOVS.startSessionCols(0), EventMethodsOVS.startSessionCols(1), EventMethodsOVS.startSessionCols.tail.tail: _*)
  }

  test("testAddStartSessionEvent") {
    val gsDF = GameSessionCreator.createGameSessionOVS(baseConfig.format("start_session"))
      .selectExpr(EventMethodsOVS.startSessionCols: _*)
    val selectedGsDF = gsOriginDF.selectExpr(EventMethodsOVS.startSessionCols: _*)

    val exCheck  = DataFrameUtils.compareDF3(gsDF.na.fill(0), selectedGsDF.na.fill(0))
    exCheck.leftRegDF.count() shouldEqual 787
    exCheck.rightRegDF.count() shouldEqual 27
    gsDF.count() shouldEqual 11826
  }

  test("testAddSpinEvent") {
    val gsDF = GameSessionCreator.createGameSessionOVS(baseConfig.format("spin"))
      .selectExpr(EventMethodsOVS.addSpinCols: _*)
    val selectedGsDF = gsOriginDF.selectExpr(EventMethodsOVS.addSpinCols: _*)

    val exCheck  = DataFrameUtils.compareDF3(gsDF.na.fill(0), selectedGsDF)
    exCheck.leftRegDF.count() shouldEqual 760
    exCheck.rightRegDF.count() shouldEqual 0

    exCheck.longMapsDF.get._1.count() shouldEqual 734
    exCheck.longMapsDF.get._2.count() shouldEqual 1

    exCheck.intMapsDF shouldBe None

    gsDF.count() shouldEqual 4872
  }

  test("testEndSessionEvent") {
    val gsDF = GameSessionCreator.createGameSessionOVS(baseConfig.format("start_session,end_session"))
      .selectExpr(EventMethodsOVS.endSessionCols: _*)
    val selectedGsDF = gsOriginDF.selectExpr(EventMethodsOVS.endSessionCols: _*)

    val typeMap = gsDF.dtypes.map(column =>
      column._2 match {
        case "IntegerType" => column._1 -> 0
        case "LongType" => column._1 -> 0
        case "StringType" => column._1 -> ""
        case "DoubleType" => column._1 -> 0.0
        case "FloatType" => column._1 -> 0.0
        case "BooleanType" => column._1 -> false
      }).toMap

    val exCheck  = DataFrameUtils.compareDF3(gsDF.na.fill(typeMap), selectedGsDF)
    exCheck.leftRegDF.count() shouldEqual 760
    exCheck.rightRegDF.count() shouldEqual 0
    gsDF.count() shouldEqual 11826
  }

  test("testCollectBonusEvent") {
    val gsDF = GameSessionCreator.createGameSessionOVS(baseConfig.format("collect_bonus"))
      .selectExpr(EventMethodsOVS.addCollectBonusCols: _*)
    val selectedGsDF = gsOriginDF.selectExpr(EventMethodsOVS.addCollectBonusCols: _*)

    val exCheck  = DataFrameUtils.compareDF3(gsDF.na.fill(0), selectedGsDF)
    exCheck.leftRegDF.count() shouldEqual 680
    exCheck.rightRegDF.count() shouldEqual 1963

    exCheck.intMapsDF.size shouldEqual 1
    exCheck.longMapsDF.size shouldEqual 0
    gsDF.count() shouldEqual 9783
  }

  test("testOpenSlotEvent") {
    val gsDF = GameSessionCreator.createGameSessionOVS(baseConfig.format("start_session,open_slot"))
      .selectExpr(EventMethodsOVS.addOpenSlotCols: _*)
    val selectedGsDF = gsOriginDF.selectExpr(EventMethodsOVS.addOpenSlotCols: _*)

    val exCheck  = DataFrameUtils.compareDF3(gsDF.na.fill(0), selectedGsDF)
    exCheck.leftRegDF.count() shouldEqual 8037
    exCheck.rightRegDF.count() shouldEqual 7278
    gsDF.count() shouldEqual 11825
  }

  test("testTransactionEvent") {
    val gsDF = GameSessionCreator.createGameSessionOVS(baseConfig.format("start_session,transaction"))
      .selectExpr(EventMethodsOVS.addTransactionCols: _*)
    val selectedGsDF = gsOriginDF.selectExpr(EventMethodsOVS.addTransactionCols: _*)

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
    exCheck.leftRegDF.count() shouldEqual 764
    exCheck.rightRegDF.count() shouldEqual 5
    gsDF.count() shouldEqual 11825
  }

  test("testCashierEvent") {
    val gsDF = GameSessionCreator.createGameSessionOVS(baseConfig.format("start_session,cashier"))
      .selectExpr(EventMethodsOVS.addCashierCols: _*)
    val selectedGsDF = gsOriginDF.selectExpr(EventMethodsOVS.addCashierCols: _*)

    val exCheck  = DataFrameUtils.compareDF3(gsDF.na.fill(0), selectedGsDF)
    exCheck.leftRegDF.count() shouldEqual 759
    exCheck.rightRegDF.count() shouldEqual 0

    exCheck.intMapsDF.size shouldEqual 1
    exCheck.longMapsDF.size shouldEqual 0
    gsDF.count() shouldEqual 11825
  }

  test("testFacebookConnectEvent") {
    val gsDF = GameSessionCreator.createGameSessionOVS(baseConfig.format("start_session,facebook_connect"))
      .selectExpr(EventMethodsOVS.addFacebookConnectCols: _*)
    val selectedGsDF = gsOriginDF.selectExpr(EventMethodsOVS.addFacebookConnectCols: _*)

    val typeMap = gsDF.dtypes.map(column =>
      column._2 match {
        case "IntegerType" => column._1 -> 0
        case "LongType" => column._1 -> 0
        case "StringType" => column._1 -> ""
        case "DoubleType" => column._1 -> 0.0
        case "FloatType" => column._1 -> 0.0
        case "BooleanType" => column._1 -> false
      }).toMap

    val exCheck  = DataFrameUtils.compareDF3(gsDF.na.fill(typeMap), selectedGsDF)
    exCheck.leftRegDF.count() shouldEqual 759
    exCheck.rightRegDF.count() shouldEqual 0
    gsDF.count() shouldEqual 11825
  }

  val baseConfig =
    s"""
       |dgn {
       |  gs {
       |    dt = "2017-05-01"
       |    hr = 10
       |    hours_back = 3
       |    join_on = "session_id"
       |    rm_map_nulls = [spin, cashier, collect_bonus]
       |    event_prefix = ""
       |    schema = ${TestsPathsUtils.getOvsInPath("schemas")}
       |    game_session = {
       |      in = ${TestsPathsUtils.getOvsInPath("event/")}
       |      out = ${TestsPathsUtils.getOvsOutPath("report/game_session")}
       |    }
       |    events = [%s]
       |  }
       |}
     """.stripMargin
}

