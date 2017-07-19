package com.slg.core.utils

import com.dgn.core.utils.DateTimeUtils.DTHR
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

/**
  * Created by ophchu on 1/1/17.
  */
object SparkUtils {

  def readDtHrDF(path: String, dthr: List[DTHR])
                (implicit sps: SparkSession): DataFrame = {
    dthr.map(dh => {
      readDF(createDtHrPath(path)(dh))
    }).reduce((df1: DataFrame, df2: DataFrame) => df1.union(df2))
  }

  def writeDF(df: DataFrame, path: String, dthr: DTHR) = {
    df.write.mode(SaveMode.Overwrite).parquet(createDtHrPath(path)(dthr))
  }

  def readDF(path: String)
            (implicit sps: SparkSession): DataFrame = {
    sps.read.option("basePath", path.split("dt=").head).parquet(path)
  }

  def createDtHrPath(path: String)(dthr: DTHR) = {
    s"${path}/dt=${dthr.dt}/hr=${dthr.hr}"
  }

}
