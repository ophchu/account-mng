package com.dgn.core.utils

import com.dgn.core.utils.DateTimeUtils.DTHR
import org.apache.spark.sql._
import com.databricks.spark.avro._
import org.apache.spark.sql.functions._
import org.slf4j.LoggerFactory

/**
  * Created by ophchu on 1/1/17.
  */
object SparkUtils {
  private val LOG = LoggerFactory.getLogger(getClass)

  def loadAndRegisterDF(tableName: String, path: String, uniqueName: Boolean = false)
                       (implicit sps: SparkSession): String = {
    loadAndRegisterDF(tableName, readDF(path), uniqueName)
  }
  def loadAndRegisterDF(tableName: String, df: DataFrame, uniqueName: Boolean)
                       (implicit sps: SparkSession): String = {
    val tName = uniqueName match {
      case true => s"${tableName}_${df.rdd.id}"
      case false => tableName
    }
    df.createOrReplaceTempView(tName)
    tName
  }

  def writeDF(df: DataFrame, path: String, dthr: DTHR) = {
    df.write.mode(SaveMode.Overwrite).parquet(createDtHrPath(path)(dthr))
  }

  def readTodayDF(rootPath: String, daysRange: Int)
                 (implicit sps: SparkSession): DataFrame = {
    readDtDF(rootPath, DateTimeUtils.getCurrentDateTime().toLocalDate.toString, daysRange)
  }

  def readDtDF(rootPath: String, dt: String, daysRange: Int, dtInclude: Boolean = true)
              (implicit sps: SparkSession): DataFrame = {
    val daysPaths = DateTimeUtils.getLocalDateDaysRange(dt, daysRange)
    val paths = daysPaths.map(day => s"dt=${day.toString}")
    SparkUtils.readDF(rootPath, paths: _*)
  }

  def readDtDF(rootPath: String, dtStart: String, dtEnd: String)
              (implicit sps: SparkSession): DataFrame = {
    val daysPaths = DateTimeUtils.getLocalDateDaysRange(dtStart, dtEnd)
    val paths = daysPaths.map(day => s"dt=${day.toString}")
    SparkUtils.readDF(rootPath, paths: _*)
  }


  def readDF(path: String, subdirs: String*)
            (implicit sps: SparkSession): DataFrame = {
    val paths = subdirs.isEmpty match {
      case true => Seq(path)
      case false => subdirs.map(sub => s"$path/$sub")
    }
    LOG.info(
      s"""
         |Going to read to following paths:
         |${paths.mkString(", ")}
       """.stripMargin)
    sps.read.option("basePath", path.split("dt=").head).parquet(paths:_*)
  }

//  def readDF(path: String)
//            (implicit sps: SparkSession): DataFrame = {
//    sps.read.option("basePath", path.split("dt=").head).parquet(path)
//  }

  def createDtHrPath(path: String)(dthr: DTHR) = {
    s"""$path/dt=${dthr.dt}/hr=${dthr.hr}"""
  }

  def writeDF(df: DataFrame, path: String, dt: String) = {
    df.write.mode(SaveMode.Overwrite).parquet(createDtPath(path)(dt))
  }

  def readAvroDtHrDF(path: String, dthr: List[DTHR])
                (implicit sps: SparkSession): DataFrame = {
    dthr.map(dh => {
      readAvroFile(createDtHrPath(path)(dh))
    }).reduce((df1: DataFrame, df2: DataFrame) => df1.union(df2))
  }

  def addDtHr(df: DataFrame) = {
    val dtFunc = udf(DateTimeUtils.getTzDT _)
    val hrFunc = udf(DateTimeUtils.getTzHR _)
      df
        .withColumn("dt", dtFunc(col("ts")))
        .withColumn("hr", hrFunc(col("ts")))
  }
  def readAvroFile(path: String)
                  (implicit sps: SparkSession): DataFrame = {
    sps.read.avro(path)
  }

  def createDtPath(path: String)(dt: String) = {
    s"""$path/dt=$dt"""
  }

}
