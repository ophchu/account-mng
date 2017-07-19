package com.dgn.core.dspt

import com.dgn.core.utils.{DataFrameUtils, DateTimeUtils}
import com.typesafe.config.ConfigFactory
import org.apache.spark.sql._
import org.json4s._
import org.json4s.jackson.JsonMethods._
import org.slf4j.LoggerFactory

import scala.collection.JavaConversions._
import scala.util.Try

/**
  * Created by ophchu on 1/1/17.
  */

object DSPTCreator {
  private val LOG = LoggerFactory.getLogger(getClass)

  case class EventTypeCount(eventType: String, dt: String, hr: Int, count: Long)

  def createDSPT(confStr: String)
                (implicit sps: SparkSession) = {
    val config = ConfigFactory.parseString(confStr)
    val dt = config.getString("dgn.dspt.dt")
    val hr = config.getString("dgn.dspt.hr")

    val kinesisInPath = config.getString("dgn.dspt.kinesis.in")
    val kinesisOutPath = config.getString("dgn.dspt.kinesis.out")
    val kinesisPartNum = Try(config.getInt("dgn.dspt.kinesis.parts")).getOrElse(32)
    val dsptPath = config.getString("dgn.dspt.dspt.out")
    val schemaPath = config.getString("dgn.dspt.dspt.schema")
    val eventsWL = Try(config.getStringList("dgn.dspt.events_wl").toList).getOrElse(List.empty[String])
    val dsptPartNum = Try(config.getInt("dgn.dspt.dspt.parts")).getOrElse(10)

    LOG.info(
      s"""
         |DSPTCreator:
         |dt=$dt, hr=$hr
         |kinesisInPath=$kinesisInPath
         |kinesisOutPath=$kinesisOutPath
         |dsptPath=$dsptPath
         |kinesisPartNum=$kinesisPartNum
         |dsptPartNum=$dsptPartNum
      """.stripMargin)

    val fetchedEvents = fetchKinesisEvents(dt, hr, kinesisInPath, kinesisOutPath, kinesisPartNum, eventsWL)

    LOG.info(
      s"""
         |Fetched ${fetchedEvents.size} events:
         |${fetchedEvents.mkString("\n")}
       """.stripMargin)

    val eventsList = eventsWL.nonEmpty match {
      case true => eventsWL
      case false => fetchedEvents.map(_.eventType)
    }

    LOG.info(
      s"""
         |Going to convert: ${eventsList.size} events:
         |${eventsList.mkString("\n")}
       """.stripMargin)

    eventsList.foreach(event =>
      convertToParquet(event, kinesisOutPath, dsptPath, schemaPath, dt, hr, dsptPartNum)
    )
  }

  def convertToParquet(
                        event: String,
                        rootPath: String,
                        tgtRootPath: String,
                        schemaPath: String,
                        dt: String,
                        hr: String,
                        dsptPartNum: Int)
                      (implicit sps: SparkSession): Unit = {
    val schema = DataFrameUtils.createStructTypeFromFile(s"$schemaPath/${event}.avsc")
    val eventDF = sps.read.schema(schema).json(s"$rootPath/eventType=$event/dt=$dt/hr=$hr")

    eventDF
      .coalesce(dsptPartNum)
      .write
      .mode(SaveMode.Overwrite)
      .parquet(s"$tgtRootPath/eventType=$event/dt=$dt/hr=$hr")

  }

  case class JsonEventWrapper(eventType: String, dt: String, hr: Int, value: String)

  def fetchKinesisEvents(
                          dt: String,
                          hr: String,
                          kinesisRootPath: String,
                          kinesisOutPath: String,
                          kinesisPartNum: Int,
                          eventsWL: List[String] = List.empty[String])
                        (implicit sps: SparkSession): List[EventTypeCount] = {
    import sps.implicits._
    sps.sparkContext.hadoopConfiguration.set("textinputformat.record.delimiter", "**##**")

    val kinesisInPaths = DateTimeUtils.formatKinesisPaths(dt, hr, -1, 1, kinesisRootPath)

    LOG.info(
      s"""
         |Load the following Kinesis paths:
         |${kinesisInPaths.mkString("\n")}
      """.stripMargin)

    val kinesisInRDD = kinesisInPaths.map(path =>
      sps.sparkContext.textFile(path).filter(_.length > 1)
    ).reduce((df1, df2) => df1.union(df2))

    //todo - filter for the write hr only
    val kinesisOutDS = kinesisInRDD.map(line => {
      val jsonMap = parse(line).values.asInstanceOf[Map[String, Any]]
      JsonEventWrapper(
        jsonMap("event_type").toString,
        DateTimeUtils.getDTFromTS(jsonMap("ts").toString.toLong),
        DateTimeUtils.getHRFromTS(jsonMap("ts").toString.toLong),
        line)
    }
    ).toDS

    val currentHRDS = kinesisOutDS.filter(s"hr=$hr and dt='$dt'")

    val eventList = currentHRDS
      .groupBy("eventType", "dt", "hr").count
      .map(row => EventTypeCount(row.getString(0), row.getString(1), row.getInt(2), row.getLong(3))).collect.toList

    val wlEventList = eventsWL.nonEmpty match {
      case true => eventsWL
      case false => eventList.map(_.eventType)
    }

    wlEventList.foreach(event => {
      val eventDF = currentHRDS.filter(s"""eventType="$event"""").select("value")
      eventDF
        .coalesce(kinesisPartNum)
        .write
        .mode(SaveMode.Overwrite)
        .text(s"""$kinesisOutPath/eventType=$event/dt=$dt/hr=$hr""")
    })

    eventList
  }


  val dsptConfig =
    """
      |dgn {
      |  dspt {
      |    dt = "2016-12-23"
      |    hr = 00
      |
      |    kinesis {
      |      in = ${PathsUtils.getAbsoultePath("/dbfs/mnt/ltsprodbackup/lts/raw/event/")}
      |      out = ${PathsUtils.getAbsoultePath("/dbfs/mnt/ltsprodbackup/lts/kinesis/event/")}
      |    }
      |
      |    dspt {
      |      out = ${PathsUtils.getAbsoultePath("src/test/resources/out/game_session")}
      |    }
      |  }
      |}
      |
    """.stripMargin

}




