import java.time.temporal.ChronoUnit

import com.dgn.core.utils.DateTimeUtils
import org.apache.spark.SparkContext
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, SparkSession}

val r = 3000000000L
r.toFloat
r.toDouble
r.toShort


val spark :SparkSession = null

val ltsGameSessionPath = "sadasd"
//spark.conf.get("inferSchema")
val df: DataFrame = null
val originDF: DataFrame = null

import spark.implicits._
import org.apache.spark.sql._
DateTimeUtils.formatKinesisPath("2017-07-15", "4", "/kinesis/root")


//df.withColumn("hr", lit("asd"))

val l = List(1,2,3)

l.take(10)
l.sum
