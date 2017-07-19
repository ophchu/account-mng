package com.dgn.core.derived.userprofile

import java.time.LocalDate

import com.dgn.core.utils.SparkUtils
import com.typesafe.config.ConfigFactory
import org.apache.spark.sql._

import scala.collection.JavaConversions._

/**
  * Created by ophchu on 1/1/17.
  */
trait UserProfileCreator {
  def createUserKPI(gsDaily: DataFrame): DataFrame
  def createUserKPIDecile(userKPI: DataFrame): DataFrame
  def createUserProfile(userKPI: DataFrame): DataFrame

}

object UserProfileCreator {
  def createUserProfileLTS(confStr: String)
                       (implicit sps: SparkSession): DataFrame = {
    createUserProfile(confStr)(UserProfileCreatorLTS)
  }

  def createUserProfile(confStr: String)
                       (upCreator: UserProfileCreator)
                       (implicit sps: SparkSession): DataFrame = {
    val config = ConfigFactory.parseString(confStr)
    val dt = config.getString("dgn.up.dt")
    val daysBack = config.getInt("dgn.up.days_back")

    val localTime = LocalDate.parse(dt)
    val prevLocalTime = localTime.minusDays(daysBack)

    val gsDailyDF =
      SparkUtils.readDF(config.getString("dgn.up.game_session_daily.in"))
      .where(s"""dt<="$dt" and dt>"${prevLocalTime.toString}"""")

    val userKpiDF = upCreator.createUserKPI(gsDailyDF)
    SparkUtils.writeDF(gsDailyDF, config.getString("dgn.up.user_kpi.out"), dt)

    val userKpiDecile = upCreator.createUserKPIDecile(userKpiDF)
    SparkUtils.writeDF(gsDailyDF, config.getString("dgn.up.user_kpi_decile.out"), dt)

    val userProfile = upCreator.createUserProfile(userKpiDF)
    SparkUtils.writeDF(gsDailyDF, config.getString("dgn.up.user_profile.out"), dt)

    userProfile
  }


}



