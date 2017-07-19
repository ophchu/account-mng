package com.dgn.core.derived.userprofile
import org.apache.spark.sql.DataFrame

/**
  * Created by ophchu on 1/15/17.
  */
object UserProfileCreatorLTS extends UserProfileCreator{
  override def createUserKPI(gsDaily: DataFrame): DataFrame = {
    gsDaily.groupBy("user_id").count()
  }

  override def createUserKPIDecile(userKPI: DataFrame): DataFrame = {
    userKPI
  }

  override def createUserProfile(userKPI: DataFrame): DataFrame = {
    userKPI
  }
}
