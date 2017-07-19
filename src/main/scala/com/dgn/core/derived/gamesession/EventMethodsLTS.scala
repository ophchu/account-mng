package com.dgn.core.derived.gamesession

import com.dgn.core.utils.SparkUtils
import org.apache.spark.sql._
import org.apache.spark.sql.types.{StructType, _}
import org.slf4j.LoggerFactory

/**
  * Created by ophchu on 1/4/17.
  */
object EventMethodsLTS extends GameSessionEventMethods {
  private val LOG = LoggerFactory.getLogger(getClass)

  override val eventToMethodList: List[EventToMethod] =
    List(
      EventToMethod("start_session", addStartSessionEvent),
      EventToMethod("spin", addSpinEvent),
      EventToMethod("collect_bonus", addCollectBonusEvent),
      EventToMethod("transaction", addTransactionEvent, transactionSpecialJoin),
      EventToMethod("open_slot", addOpenSlotEvent),
      EventToMethod("cashier", addCashierEvent),
      EventToMethod("facebook_connect", addFacebookConnectEvent),
      EventToMethod("back_to_lobby", addBackToLobbyEvent),
      EventToMethod("bonus_game", addBonusGameEvent),
      EventToMethod("facebook_disconnect", addFacebookDisconnectEvent),
      EventToMethod("inbox", addInboxEvent),
      EventToMethod("invite", addInviteEvent),
      EventToMethod("mission", addMissionEvent),
      EventToMethod("recommendation", addRecommendationEvent),
      EventToMethod("request_gift", addRequestGiftEvent),
      EventToMethod("send_gift", addSendGiftEvent),
      EventToMethod("share", addShareEvent),
      EventToMethod("stop", addStopEvent),
      EventToMethod("end_session", addEndSessionEvent)
    )

  def convertColumnToMap(str: String): Map[String, Long] = {
    val resultMap: Map[String, Long] = str.substring(0, str.length)
      .split(";")
      .map(_.split(":"))
      .map { case Array(k, v) => (k.substring(0, k.length), v.substring(0, v.length).toLong) }
      .toMap
    resultMap
  }


  val startSessionCols = List(
    "session_id",
    "ts",
    "user_id",
    "platform_id",
    "balance",
    "level",
    "xp",
    "vip_level",
    "gender",
    "age",
    "country_code",
    "country_tier",
    "session_count",
    "purchase_count",
    "purchase_amount",
    "first_play_ts",
    "install_ts",
    "device_id",
    "os",
    "model",
    "ip",
    "retention_organic_source",
    "retention_campaign_id",
    "retention_campaign_source",
    "retention_ad_id",
    "retention_ad_creative_id",
    "version",
    "browser_major_version",
    "browser_minor_version",
    "browser_type",
    "regular_slots_available",
    "featured_slots_available",
    "new_slots_available",
    "lucky_time_slot_id",
    "fb_game_friends",
    "platform_user_id",
    "first_deposit_ts",
    "payer"
  )

  def addStartSessionEvent(eventDF: DataFrame, currentGsDF: DataFrame): DataFrame = {
    addMissingFileds(eventDF, startSessionCols, "start_session").selectExpr(startSessionCols: _*)
  }

  def addMissingFileds(df: DataFrame, expectedFields: List[String], eventName: String): DataFrame = {
    val existsFileds = df.schema.fields.map(_.name)
    val missingFields = expectedFields.diff(existsFileds)
    LOG.warn(
      s"""
         |$eventName missing fileds:
         |${missingFields.mkString(", ")}
         |Add to the dataframe!!!!
      """.stripMargin)

    import org.apache.spark.sql.functions._
    missingFields.foldLeft(df)((df1, name) => df1.withColumn(name, lit("")))
  }

  val endSessionCols = List(
    "session_id"
    , "duration"
    , "level_gain"
    , "xp_gain"
    , "balance_gain"
    , "vip_level_gain"
    , "vip_points_gain"
    , "days_from_install"
  )
  val endSessionEventCols = List(
    "session_id",
    "last_activity_ts",
    "level",
    "xp",
    "balance",
    "vip_level",
    "vip_points"
  )

  def addEndSessionEvent(eventDF: DataFrame, currentGsDF: DataFrame): DataFrame = {
    val joinedFields = List(
      "eventDF.session_id"
      , "eventDF.last_activity_ts"
      , "currentGsDF.ts as ts"
      , "eventDF.level as eventDF_level"
      , "currentGsDF.level as currentGsDF_level"
      , "eventDF.xp as eventDF_xp"
      , "currentGsDF.xp as currentGsDF_xp"
      , "eventDF.balance as eventDF_balance"
      , "currentGsDF.balance as currentGsDF_balance"
      , "eventDF.vip_level as eventDF_vip_level"
      , "currentGsDF.vip_level as currentGsDF_vip_level"
      , "eventDF.vip_points as eventDF_vip_points"
      , "currentGsDF.first_play_ts as currentGsDF_first_play_ts"
    )

    eventDF.alias("eventDF").join(currentGsDF.alias("currentGsDF"), "session_id")
      .selectExpr(joinedFields: _*).createOrReplaceTempView("addEndSessionTmpView")
    val sqlText =
      """
        |select session_id,
        |last_activity_ts - ts as duration,
        |eventDF_level - currentGsDF_level as level_gain,
        |eventDF_xp - currentGsDF_xp as xp_gain,
        |eventDF_balance - currentGsDF_balance as balance_gain,
        |nvl(eventDF_vip_level - currentGsDF_vip_level, 0) as vip_level_gain,
        |nvl(eventDF_vip_points, 0) as vip_points_gain,
        |cast(FLOOR((ts - currentGsDF_first_play_ts) / (1000*60*60*24)) as INT) as days_from_install
        |from addEndSessionTmpView
      """.stripMargin
    val endSessionDF = eventDF.sparkSession.sql(sqlText)
    endSessionDF
  }

  val addSpinCols = List(
    "session_id"
    , "bet_amount"
    , "win_amount"
    , "mission_win_amount"
    , "jackpot_win_amount"
    , "feature_win_amount"
    , "free_spin_count"
    , "spin_count"
    , "spin"
  )

  def addSpinEvent(eventDF: DataFrame, currentGsDF: DataFrame): DataFrame = {
    eventDF.sparkSession.udf.register("convertColumnToMap", convertColumnToMap _)

    val addSpinSql = List(
      "session_id"
      , "NVL(bet_amount,0) as bet_amount"
      , "NVL(win_amount,0) as win_amount"
      , "NVL(mission_win_amount,0) as mission_win_amount"
      , "NVL(jackpot_win_amount,0) as jackpot_win_amount"
      , "NVL(feature_win_amount,0) as feature_win_amount"
      , "NVL(CASE WHEN bet_amount = 0 THEN 1 ELSE 0 END,0) AS free_spin_count"
    )

    eventDF.select("session_id", "slot_id").createOrReplaceTempView("addSpinTmpView")
    val sqlText =
      """
        |with a as
        |(select session_id, concat(slot_id,':', count(slot_id))
        |as c_spins from addSpinTmpView
        |group by session_id, slot_id order by 1)
        |select session_id, convertColumnToMap(concat_ws(';', collect_list(c_spins))) as spin from a group by 1
      """.stripMargin

    val spinsPerSession = eventDF.sparkSession.sql(sqlText)

    val schema = StructType(List(
      StructField("session_id", StringType, nullable = false),
      StructField("bet_amount", LongType, nullable = false),
      StructField("win_amount", LongType, nullable = false),
      StructField("mission_win_amount", LongType, nullable = false),
      StructField("jackpot_win_amount", LongType, nullable = false),
      StructField("feature_win_amount", LongType, nullable = false),
      StructField("free_spin_count", LongType, nullable = false),
      StructField("spin", MapType(StringType, LongType, valueContainsNull = true), nullable = true),
      StructField("spin_count", LongType, nullable = false)))

    val spinCountDF = addToEventFieldCount(eventDF, "spin_count")
    eventDF.sparkSession.createDataFrame(eventDF.selectExpr(addSpinSql: _*)
      .groupBy("session_id")
      .sum()
      .join(spinsPerSession, "session_id")
      .join(spinCountDF, "session_id")
      .toDF(addSpinCols: _*).rdd, schema)
  }

  val addCollectBonusCols = List(
    "session_id",
    "collect_bonus",
    "collect_bonus_coins"
  )

  def addCollectBonusEvent(eventDF: DataFrame, currentGsDF: DataFrame): DataFrame = {
    eventDF.sparkSession.udf.register("convertColumnToMap", convertColumnToMap _)

    val addCollectBonusSql = List(
      "session_id"
      , "bonus_source"
      , "NVL(coins_amount,0) as coins_amount"
    )
    eventDF.selectExpr(addCollectBonusSql: _*).createOrReplaceTempView("addCollectBonusTmpView")
    val sqlText =
      """
        |with a as
        |(select session_id, concat(bonus_source,':', sum(coins_amount))
        |as c_collectBonus from addCollectBonusTmpView
        |group by session_id, bonus_source order by 1)
        |select session_id,
        |convertColumnToMap(concat_ws(';', collect_list(c_collectBonus))) as collect_bonus
        |from a
        |group by 1
      """.stripMargin
    val collectBonusEventDF = eventDF.sparkSession.sql(sqlText)

    val schema = StructType(List(
      StructField("session_id", StringType, nullable = true),
      StructField("collect_bonus_coins", LongType, nullable = true),
      StructField("collect_bonus", MapType(StringType, LongType, valueContainsNull = true), nullable = true)))

    eventDF.sparkSession.createDataFrame(eventDF.select("session_id", "coins_amount")
      .groupBy("session_id")
      .sum()
      .join(collectBonusEventDF, "session_id")
      .toDF(addCollectBonusCols: _*).rdd, schema)
  }

  val addTransactionCols = List(
    "session_id",
    "first_deposit_ts",
    "payer",
    "transaction_count",
    "transaction_amount",
    "transaction_coins"
  )

  def addTransactionEvent(eventDF: DataFrame, currentGsDF: DataFrame): DataFrame = {
    val tnsTableName = SparkUtils.loadAndRegisterDF("tns_table", eventDF, true)(eventDF.sparkSession)

    val sqlText =
      s"""
         |select
         |session_id,
         |min(ts) as first_deposit_ts,
         |true as payer,
         |count(event_id) as transaction_count,
         |sum(price) as transaction_amount,
         |sum(coins_amount) as transaction_coins
         |from $tnsTableName
         |group by session_id
      """.stripMargin

    eventDF.sparkSession.sql(sqlText)
  }


  def transactionSpecialJoin(transactionDF: DataFrame, gsDF: DataFrame, joinOn: String)
                                    : DataFrame = {
    implicit val sps = gsDF.sparkSession
    val gsTableName = SparkUtils.loadAndRegisterDF("gs", gsDF, true)
    val tnsTableName = SparkUtils.loadAndRegisterDF("tns", transactionDF, true)

    val sqlQuery =
      s"""
         |select
         |  gs.*,
         |  case
         |    when tns.payer is null and gs.payer is null then false
         |    when tns.payer is null then gs.payer else true
         |  end as new_payer,
         |  case
         |    when gs.first_deposit_ts is null or gs.first_deposit_ts = 0 then tns.first_deposit_ts
         |    when tns.first_deposit_ts < gs.first_deposit_ts then tns.first_deposit_ts
         |    else gs.first_deposit_ts
         |  end as new_first_deposit_ts,
         |  tns.transaction_count as transaction_count,
         |  tns.transaction_amount as transaction_amount,
         |  tns.transaction_coins as transaction_coins
         |from $gsTableName gs
         |left join $tnsTableName tns
         |on gs.session_id = tns.session_id
      """.stripMargin

    val withPayerDF =
      sps.sql(sqlQuery)
      .drop("payer").withColumnRenamed("new_payer", "payer")
      .drop("first_deposit_ts").withColumnRenamed("new_first_deposit_ts", "first_deposit_ts")
//    withPayerDF.join(transactionDF.drop("payer").drop("first_deposit_ts").na.fill(0), Seq(joinOn), "left")
    withPayerDF.na.fill(0)
  }

  val addOpenSlotCols = List(
    "session_id",
    "open_slot",
    "open_slot_count"
  )

  def addOpenSlotEvent(eventDF: DataFrame, currentGsDF: DataFrame): DataFrame = {
    eventDF.sparkSession.udf.register("convertColumnToMap", convertColumnToMap _)

    eventDF.select("session_id", "slot_id").createOrReplaceTempView("addOpenSlotTmpView")
    val sqlText =
      """
        |with a as
        |(select session_id, concat(slot_id,':', count(slot_id))
        |as c_slots from addOpenSlotTmpView
        |group by session_id, slot_id order by 1)
        |select session_id, convertColumnToMap(concat_ws(';', collect_list(c_slots))) as open_slot from a group by 1
      """.stripMargin

    val schema = StructType(List(
      StructField("session_id", StringType, nullable = true),
      StructField("open_slot", MapType(StringType, LongType, valueContainsNull = true), nullable = true),
      StructField("open_slot_count", LongType, nullable = true)))

    val openSlotsCountDF = addToEventFieldCount(eventDF, "open_slot_count")
    eventDF.sparkSession.createDataFrame(eventDF.sparkSession.sql(sqlText)
      .join(openSlotsCountDF, "session_id")
      .toDF(addOpenSlotCols: _*).rdd, schema)
  }

  val addCashierCols = List(
    "session_id",
    "cashier_count"
  )

  def addCashierEvent(eventDF: DataFrame, currentGsDF: DataFrame): DataFrame = {
    val result = addToEventFieldCount(eventDF, "cashier_count")
    result
  }

  val addFacebookConnectCols = List(
    "session_id",
    "facebook_connect_count"
  )

  def addFacebookConnectEvent(eventDF: DataFrame, currentGsDF: DataFrame): DataFrame = {
    addToEventFieldCount(eventDF, "facebook_connect_count")
  }

  val addBackToLobbyCols = List(
    "session_id",
    "back_to_lobby",
    "back_to_lobby_count"
  )

  def addBackToLobbyEvent(eventDF: DataFrame, currentGsDF: DataFrame): DataFrame = {
    eventDF.sparkSession.udf.register("convertColumnToMap", convertColumnToMap _)
    eventDF.select("session_id", "slot_id").createOrReplaceTempView("addBackToLobbyTmpView")
    val sqlText =
      """
        |with a as
        |(select session_id, concat(slot_id,':', count(slot_id))
        |as c_backToLobby from addBackToLobbyTmpView
        |group by session_id, slot_id order by 1)
        |select session_id, convertColumnToMap(concat_ws(';', collect_list(c_backToLobby))) as back_to_lobby from a group by 1
      """.stripMargin
    val schema = StructType(List(
      StructField("session_id", StringType, nullable = true),
      StructField("back_to_lobby", MapType(StringType, LongType, valueContainsNull = true), nullable = true),
      StructField("back_to_lobby_count", LongType, nullable = true)))

    val backToLobbyCountDF = addToEventFieldCount(eventDF, "back_to_lobby_count")
    eventDF.sparkSession.createDataFrame(eventDF.sparkSession.sql(sqlText)
      .join(backToLobbyCountDF, "session_id")
      .toDF(addBackToLobbyCols: _*).rdd, schema)
  }

  val addBonusGameCols = List(
    "session_id",
    "bonus_game",
    "bonus_game_count"
  )

  def addBonusGameEvent(eventDF: DataFrame, currentGsDF: DataFrame): DataFrame = {
    eventDF.sparkSession.udf.register("convertColumnToMap", convertColumnToMap _)
    eventDF.select("session_id", "slot_id").createOrReplaceTempView("addBonusGameTmpView")
    val sqlText =
      """
        |with a as
        |(select session_id, concat(slot_id,':', count(slot_id))
        |as c_bonus_game from addBonusGameTmpView
        |group by session_id, slot_id order by 1)
        |select session_id, convertColumnToMap(concat_ws(';', collect_list(c_bonus_game))) as bonus_game from a group by 1
      """.stripMargin

    val schema = StructType(List(
      StructField("session_id", StringType, nullable = true),
      StructField("bonus_game", MapType(StringType, LongType, valueContainsNull = true), nullable = true),
      StructField("bonus_game_count", LongType, nullable = true)))

    val bonusGameCountDF = addToEventFieldCount(eventDF, "bonus_game_count")
    eventDF.sparkSession.createDataFrame(eventDF.sparkSession.sql(sqlText)
      .join(bonusGameCountDF, "session_id")
      .toDF(addBonusGameCols: _*).rdd, schema)
  }

  val addFacebookDisconnectCols = List(
    "session_id",
    "facebook_disconnect_count"
  )

  def addFacebookDisconnectEvent(eventDF: DataFrame, currentGsDF: DataFrame): DataFrame = {
    addToEventFieldCount(eventDF, "facebook_disconnect_count")
  }

  val addInboxCols = List(
    "session_id",
    "inbox_count"
  )

  def addInboxEvent(eventDF: DataFrame, currentGsDF: DataFrame): DataFrame = {
    addToEventFieldCount(eventDF, "inbox_count")
  }

  val addInviteCols = List(
    "session_id",
    "invite_count"
  )

  def addInviteEvent(eventDF: DataFrame, currentGsDF: DataFrame): DataFrame = {
    addToEventFieldCount(eventDF, "invite_count")
  }

  val addMissionCols = List(
    "session_id",
    "mission_start_count",
    "mission_end_count",
    "mission"
  )

  def addMissionEvent(eventDF: DataFrame, currentGsDF: DataFrame): DataFrame = {
    eventDF.sparkSession.udf.register("convertColumnToMap", convertColumnToMap _)
    eventDF.select("session_id", "slot_id").createOrReplaceTempView("addMissionTmpView")
    val sqlText =
      """
        |with a as
        |(select session_id, concat(slot_id,':', count(slot_id))
        |as c_mission from addMissionTmpView
        |group by session_id, slot_id order by 1)
        |select session_id, convertColumnToMap(concat_ws(';', collect_list(c_mission))) as mission from a group by 1
      """.stripMargin

    val schema = StructType(List(
      StructField("session_id", StringType, nullable = true),
      StructField("mission_start_count", LongType, nullable = true),
      StructField("mission_end_count", LongType, nullable = true),
      StructField("mission", MapType(StringType, LongType, valueContainsNull = true), nullable = true)))

    val missionStartCountDF = addToEventFieldCount(eventDF.where("start = true"), "mission_start_count")
    val missionEndCountDF = addToEventFieldCount(eventDF.where("start != true"), "mission_end_count")

    eventDF.sparkSession.createDataFrame(eventDF.sparkSession.sql(sqlText)
      .join(missionStartCountDF, "session_id")
      .join(missionEndCountDF, "session_id")
      .toDF(addMissionCols: _*).rdd, schema)
  }

  val addRecommendationCols = List(
    "session_id",
    "recommendation",
    "recommendation_count",
    "recommendation_accept_count"
  )

  def addRecommendationEvent(eventDF: DataFrame, currentGsDF: DataFrame): DataFrame = {
    eventDF.sparkSession.udf.register("convertColumnToMap", convertColumnToMap _)
    eventDF.select("session_id", "slot_id").createOrReplaceTempView("addRecommendationTmpView")
    val sqlText =
      """
        |with a as
        |(select session_id, concat(slot_id,':', count(slot_id))
        |as c_recommendation from addRecommendationTmpView
        |group by session_id, slot_id order by 1)
        |select session_id,
        |convertColumnToMap(concat_ws(';', collect_list(c_recommendation))) as recommendation from a group by 1
      """.stripMargin

    val schema = StructType(List(
      StructField("session_id", StringType, nullable = true),
      StructField("recommendation", MapType(StringType, LongType, valueContainsNull = true), nullable = true),
      StructField("recommendation_count", LongType, nullable = true),
      StructField("recommendation_accept_count", LongType, nullable = true)))

    val recommendationCountDF = addToEventFieldCount(eventDF, "recommendation_count")
    val recommendationAcceptDF = addToEventFieldCount(eventDF.where("accept = true"), "recommendation_accept_count")

    eventDF.sparkSession.createDataFrame(eventDF.sparkSession.sql(sqlText)
      .join(recommendationCountDF, "session_id")
      .join(recommendationAcceptDF, "session_id")
      .toDF(addRecommendationCols: _*).rdd, schema)
  }

  val addRequestGiftCols = List(
    "session_id",
    "request_gift_count"
  )

  def addRequestGiftEvent(eventDF: DataFrame, currentGsDF: DataFrame): DataFrame = {
    addToEventFieldCount(eventDF, "request_gift_count")
  }

  val addSendGiftCols = List(
    "session_id",
    "send_gift_count"
  )

  def addSendGiftEvent(eventDF: DataFrame, currentGsDF: DataFrame): DataFrame = {
    addToEventFieldCount(eventDF, "send_gift_count")
  }

  val addShareCols = List(
    "session_id",
    "share_count"
  )

  def addShareEvent(eventDF: DataFrame, currentGsDF: DataFrame): DataFrame = {
    addToEventFieldCount(eventDF, "share_count")
  }

  val addStopCols = List(
    "session_id",
    "stop",
    "stop_count"
  )

  def addStopEvent(eventDF: DataFrame, currentGsDF: DataFrame): DataFrame = {
    eventDF.sparkSession.udf.register("convertColumnToMap", convertColumnToMap _)
    eventDF.select("session_id", "slot_id").createOrReplaceTempView("addStopTmpView")
    val sqlText =
      """
        |with a as
        |(select session_id, concat(slot_id,':', count(slot_id))
        |as c_stop from addStopTmpView
        |group by session_id, slot_id order by 1)
        |select session_id, convertColumnToMap(concat_ws(';', collect_list(c_stop))) as stop from a group by 1
      """.stripMargin

    val schema = StructType(List(
      StructField("session_id", StringType, nullable = true),
      StructField("stop", MapType(StringType, LongType, valueContainsNull = true), nullable = true),
      StructField("stop_count", LongType, nullable = true)))

    val stopCountDF = addToEventFieldCount(eventDF, "stop_count")
    eventDF.sparkSession.createDataFrame(eventDF.sparkSession.sql(sqlText)
      .join(stopCountDF, "session_id")
      .toDF(addStopCols: _*).rdd, schema)
  }

  private def addToEventFieldCount(df: DataFrame, eventCountFieldName: String): DataFrame = {
    df.select("session_id", "event_id")
      .groupBy("session_id")
      .count()
      .na.fill(0)
      .withColumnRenamed("count", eventCountFieldName)
  }
}
