package com.dgn.spark.utils

import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by ophchu on 11/24/16.
  */
object SparkTestUtils {
  def createSparkContext(appName: String = "test-sc") = {
    val conf = new SparkConf().setAppName(appName).setMaster("local[4]")
    new SparkContext(conf)
  }
  def createSparkSession(appName: String = "test-sc") = {
    SparkSession
      .builder()
      .appName(appName)
        .config("spark.master", "local")
      .enableHiveSupport()
      .getOrCreate()
  }
}
