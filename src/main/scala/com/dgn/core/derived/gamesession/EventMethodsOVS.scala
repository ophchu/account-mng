package com.dgn.core.derived.gamesession

import com.dgn.core.utils.SparkUtils
import org.apache.spark.sql._
import org.apache.spark.sql.types.{StructType, _}

/**
  * Created by tamirm on 22/5/17.
  */
object EventMethodsOVS extends GameSessionEventMethods{
  override val eventToMethodList: List[EventToMethod] =
    List(
      EventToMethod("start_session", addStartSessionEvent),
      EventToMethod("end_session", addEndSessionEvent),
      EventToMethod("spin", addSpinEvent),
      EventToMethod("collect_bonus", addCollectBonusEvent),
      EventToMethod("transaction", addTransactionEvent, transactionSpecialJoin),
      EventToMethod("open_slot", addOpenSlotEvent),
      EventToMethod("cashier", addCashierEvent),
      EventToMethod("facebook_connect", addFacebookConnectEvent)
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
    "ts",
    "session_id",
    "user_id",
    "platform_id",
    "version",
    "browser_major_version",
    "browser_minor_version",
    "browser_type",
    "level",
    "xp",
    "balance",
    "ip",
    "fb_game_friends",
    "vip_level",
    "slots_available",
    "payer",
    "register_campaign_source",
    "register_campaign_id",
    "register_organic_source",
    "retention_campaign_source",
    "retention_campaign_id",
    "retention_organic_source",
    "marketing_device_id",
    "country_code",
    "first_play_ts",
    "first_deposit_ts"
  )
  def addStartSessionEvent(eventDF: DataFrame, currentGsDF: DataFrame): DataFrame = {
    eventDF.selectExpr(startSessionCols: _*)
  }

  val endSessionCols = List(
    "session_id"
    ,"duration"
    ,"level_gain"
    ,"xp_gain"
    ,"balance_gain"
    ,"retention_2"
    ,"retention_7"
    ,"retention_28"
  )

  val endSessionEventCols = List(
    "session_id",
    "last_activity_ts",
    "level",
    "xp",
    "balance"
  )

  def addEndSessionEvent(eventDF: DataFrame, currentGsDF: DataFrame): DataFrame = {
    val joinedFields= List(
      "eventDF.session_id"
      ,"eventDF.last_activity_ts"
      ,"currentGsDF.ts as ts"
      ,"eventDF.level as eventDF_level"
      ,"currentGsDF.level as currentGsDF_level"
      ,"eventDF.xp as eventDF_xp"
      ,"currentGsDF.xp as currentGsDF_xp"
      ,"eventDF.balance as eventDF_balance"
      ,"currentGsDF.balance as currentGsDF_balance"
      ,"currentGsDF.first_play_ts as currentGsDF_first_play_ts"
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
        |case when cast(FLOOR((ts - currentGsDF_first_play_ts) / (1000*60*60*24)) as INT) = 1 then true else false end as retention_2,
        |case when cast(FLOOR((ts - currentGsDF_first_play_ts) / (1000*60*60*24)) as INT) = 6 then true else false end as retention_7,
        |case when cast(FLOOR((ts - currentGsDF_first_play_ts) / (1000*60*60*24)) as INT) = 27 then true else false end as retention_28
        |from addEndSessionTmpView
      """.stripMargin
    val endSessionDF = eventDF.sparkSession.sql(sqlText)
    endSessionDF
  }

  val addSpinCols = List(
    "session_id"
    ,"bet_amount"
    ,"default_bet_amount"
    ,"win_amount"
    ,"spins"
    ,"slots_played"
  )

  def addSpinEvent(eventDF: DataFrame, currentGsDF: DataFrame): DataFrame = {
    eventDF.sparkSession.udf.register("convertColumnToMap", convertColumnToMap _)

    val addSpinSql = List(
      "session_id"
      ,"NVL(bet_amount,0) as bet_amount"
      ,"NVL(default_bet,0) as default_bet_amount"
      ,"NVL(win_amount,0) as win_amount"
    )

    eventDF.select("session_id", "slot_id").createOrReplaceTempView("addSpinTmpView")
    val sqlText =
      """
        |with a as
        |(select session_id, concat(slot_id,':', count(slot_id))
        |as c_spins from addSpinTmpView
        |group by session_id, slot_id order by 1)
        |select session_id, convertColumnToMap(concat_ws(';', collect_list(c_spins))) as slots_played from a group by 1
      """.stripMargin

    val slotsPlayed = eventDF.sparkSession.sql(sqlText)

    val schema = StructType(List(
      StructField("session_id", StringType, nullable = true),
      StructField("bet_amount", LongType, nullable = true),
      StructField("default_bet_amount", LongType, nullable = true),
      StructField("win_amount", LongType, nullable = true),
      StructField("spins", LongType, nullable = true),
      StructField("slots_played", MapType(StringType, LongType, valueContainsNull = true), nullable = true)))

    val spinsCountDF = addToEventFieldCount(eventDF, "spins")

    eventDF.sparkSession.createDataFrame(eventDF.selectExpr(addSpinSql: _*)
      .groupBy("session_id")
      .sum()
      .join(spinsCountDF, "session_id")
      .join(slotsPlayed, "session_id")
      .toDF(addSpinCols: _*).rdd, schema)
  }

  val addCollectBonusCols = List(
    "session_id",
    "collect_bonus"
  )
  def addCollectBonusEvent(eventDF: DataFrame, currentGsDF: DataFrame): DataFrame = {
    eventDF.sparkSession.udf.register("convertColumnToMap", convertColumnToMap _)

    val addCollectBonusSql = List(
    "session_id"
    ,"bounus_source as bonus_source"
    ,"NVL(coins_amount,0) as coins_amount"
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
      StructField("collect_bonus", MapType(StringType, LongType, valueContainsNull = true), nullable = true)))

    eventDF.sparkSession.createDataFrame(eventDF.select("session_id")
      .groupBy("session_id")
      .sum()
      .join(collectBonusEventDF, "session_id")
      .toDF(addCollectBonusCols: _*).rdd, schema)
  }

  val addTransactionCols = List(
    "session_id",
    "deposits",
    "deposits_amount",
    "deposits_coins",
    "ftd",
    "first_deposit_ts"
  )
  def addTransactionEvent(eventDF: DataFrame, currentGsDF: DataFrame): DataFrame = {
    val addTransactionSql = List(
      "session_id"
      ,"event_id"
      ,"price"
      ,"coins_amount"
      ,"purchase_count"
      ,"ts"
    )

    eventDF.selectExpr(addTransactionSql: _*).createOrReplaceTempView("addTransactionTmpView")
    val sqlText =
      """
        |select session_id,
        |count(event_id) as deposits,
        |sum(price) as deposits_amount,
        |max(true) as ftd,
        |sum(coins_amount) as deposits_coins,
        |max(case when purchase_count=0 then ts end) as first_deposit_ts
        |from addTransactionTmpView
        |group by session_id order by 1
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
         |    when tns.ftd is null and ftd is null then false
         |    when tns.ftd is null then ftd else true
         |  end as new_ftd,
         |  case
         |    when gs.first_deposit_ts is not null and gs.first_deposit_ts < tns.first_deposit_ts then gs.first_deposit_ts
         |    when tns.first_deposit_ts is null then gs.first_deposit_ts
         |    else tns.first_deposit_ts
         |  end as new_first_deposit_ts,
         |  tns.deposits as deposits,
         |  tns.deposits_amount as deposits_amount,
         |  tns.deposits_coins as deposits_coins
         |from $gsTableName gs
         |left join $tnsTableName tns
         |on gs.session_id = tns.session_id
      """.stripMargin

    val withPayerDF =
      sps.sql(sqlQuery)
        .drop("ftd").withColumnRenamed("new_ftd", "ftd")
        .drop("first_deposit_ts").withColumnRenamed("new_first_deposit_ts", "first_deposit_ts")
    withPayerDF.na.fill(0)
  }

  val addOpenSlotCols = List(
    "session_id",
    "open_slots"
  )
  def addOpenSlotEvent(eventDF: DataFrame, currentGsDF: DataFrame): DataFrame = {

    eventDF.select("session_id", "slot_id").createOrReplaceTempView("addOpenSlotTmpView")
    val sqlText =
      """
        |SELECT session_id,
        |collect_list(slot_id) open_slots
        |FROM addOpenSlotTmpView
        |GROUP BY 1
      """.stripMargin

    val schema = StructType(List(
      StructField("session_id", StringType, nullable = true),
      StructField("open_slots", ArrayType(StringType), nullable = true)))

    eventDF.sparkSession.createDataFrame(eventDF.sparkSession.sql(sqlText)
      .toDF(addOpenSlotCols: _*).rdd, schema)
  }

  val addCashierCols = List(
    "session_id",
    "cashier"
  )
  def addCashierEvent(eventDF: DataFrame, currentGsDF: DataFrame): DataFrame = {
    eventDF.sparkSession.udf.register("convertColumnToMap", convertColumnToMap _)

    val addCashierSql = List(
      "session_id"
      ,"cashier_source"
      ,"1 as cashier_amount"
    )
    eventDF.selectExpr(addCashierSql: _*).createOrReplaceTempView("addCashierTmpView")
    val sqlText =
      """
        |with a as
        |(select session_id, concat(cashier_source,':', sum(cashier_amount))
        |as c_cashier from addCashierTmpView
        |group by session_id, cashier_source order by 1)
        |select session_id,
        |convertColumnToMap(concat_ws(';', collect_list(c_cashier))) as cashier
        |from a
        |group by 1
      """.stripMargin
    val cashierEventDF = eventDF.sparkSession.sql(sqlText)

    val schema = StructType(List(
      StructField("session_id", StringType, nullable = true),
      StructField("cashier", MapType(StringType, LongType, valueContainsNull = true), nullable = true)))

    eventDF.sparkSession.createDataFrame(eventDF.select("session_id")
      .groupBy("session_id")
      .sum()
      .join(cashierEventDF, "session_id")
      .toDF(addCashierCols: _*).rdd, schema)
  }

  val addFacebookConnectCols = List(
    "session_id",
    "true as facebook_connect"
  )
  def addFacebookConnectEvent(eventDF: DataFrame, currentGsDF: DataFrame): DataFrame = {
    eventDF.selectExpr(addFacebookConnectCols: _*)
  }

  private def addToEventFieldCount(df: DataFrame, eventCountFieldName: String): DataFrame = {
    df.select("session_id", "event_id")
      .groupBy("session_id")
      .count()
      .withColumnRenamed("count", eventCountFieldName)
  }
}