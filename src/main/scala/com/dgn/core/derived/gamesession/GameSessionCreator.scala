package com.dgn.core.derived.gamesession

import com.dgn.core.utils._
import com.typesafe.config.ConfigFactory
import org.apache.spark.sql._
import org.slf4j.LoggerFactory

import scala.collection.JavaConversions._
import scala.util.Try
import org.apache.spark.sql.functions._

/**
  * Created by ophchu on 1/1/17.
  */
case class EventToMethod(
                          eventName: String,
                          eventMethod: (DataFrame, DataFrame) => DataFrame,
                          joinMethod: (DataFrame, DataFrame, String) => DataFrame
                        )


object EventToMethod {
  def apply(eventName: String,
             eventMethod: (DataFrame, DataFrame) => DataFrame): EventToMethod = {
    EventToMethod(eventName, eventMethod, SimpleJoin)
  }
  def SimpleJoin(eventDF: DataFrame, gsDF: DataFrame, joinOn: String) = {
    gsDF.join(eventDF, Seq(joinOn), "left")
  }
}
trait GameSessionEventMethods {
  val eventToMethodList: List[EventToMethod]
}

object GameSessionCreator {
  private val LOG = LoggerFactory.getLogger(getClass)

  def createGameSessionLTS(confStr: String)
                          (implicit sps: SparkSession): DataFrame = {
    createGameSession(confStr, EventMethodsLTS)
  }

  def createGameSessionOVS(confStr: String)
                          (implicit sps: SparkSession): DataFrame = {
    createGameSession(confStr, EventMethodsOVS)
  }

  def createGameSession(confStr: String, eventMethods: GameSessionEventMethods)
                       (implicit sps: SparkSession): DataFrame = {
    val config = ConfigFactory.parseString(confStr)
    val dt = config.getString("dgn.gs.dt")
    val hr = config.getString("dgn.gs.hr")
    val hours_back = config.getInt("dgn.gs.hours_back")
    val joinOn = config.getString("dgn.gs.join_on")
    val eventPrefix = Try(config.getString("dgn.gs.event_prefix")).getOrElse("eventType=")
    val gsPathIn = config.getString("dgn.gs.game_session.in")
    val gsPathOut = config.getString("dgn.gs.game_session.out")
    val schemaPath = config.getString("dgn.gs.schema")
    val partitionNum = Try(config.getInt("dgn.gs.parts")).getOrElse(10)

    val rmMapNulls = Try(config.getStringList("dgn.gs.rm_map_nulls").toList).getOrElse(List.empty[String])

    LOG.info(
      s"""

         |Create Game Session:
         |---------------------
         |dt: $dt
         |hr: $hr
         |gsIn: $gsPathIn
         |gsOut: $gsPathOut
         |hoursBack: $hours_back
         |join_on: $joinOn
         |eventPrefix: $eventPrefix
         |schemaPath: $schemaPath
         |rmMapNulls: ${rmMapNulls.mkString(", ")}
         |partitionNum: $partitionNum
       """.stripMargin)

    val currentDtHr = List(DateTimeUtils.DTHR(dt, hr))

    val endSessionIdsDF = SparkUtils.readDF(s"$gsPathIn/${eventPrefix}end_session/dt=$dt/hr=$hr")
      .select("session_id")

    val sidTableName = SparkUtils.loadAndRegisterDF(
      "sid_table",
      endSessionIdsDF,
      uniqueName = true
    )

    val sidFilterMethod = filterSids(sidTableName, dt, hr, hours_back) _

    val reqEvents =
      Try(config.getStringList("dgn.gs.events").toList).getOrElse {
        FileUtils.ls(s"/dbfs/$gsPathIn").map(fileName => {
          eventPrefix.isEmpty match {
            case true => fileName.getFileName.toString
            case false => fileName.getFileName.toString.split(eventPrefix)(1)
          }
        })
      }
    val blackedListedEvent = Try(config.getStringList("dgn.gs.black_events").toList).getOrElse(List.empty[String])

    val eventList = reqEvents
      .filterNot(blackedListedEvent contains)
      .filter(eventMethods.eventToMethodList.map(_.eventName) contains)

    val eventMethodList = eventMethods.eventToMethodList.filter(eventMethod => eventList.contains(eventMethod.eventName))

    val gameSessionNoSchemaDF = eventMethodList.tail.foldLeft {
      val firstEventDF = sidFilterMethod(s"$gsPathIn/$eventPrefix${eventMethodList.head.eventName}")
      val firstEventGSDF = eventMethodList.head.eventMethod(firstEventDF, firstEventDF).na.fill(0)
      endSessionIdsDF.join(firstEventGSDF, Seq(joinOn), "left")
    }((gsDF, nextEvent) => {
      val nextEventDF = sidFilterMethod(s"$gsPathIn/$eventPrefix${nextEvent.eventName}")
      val res444 = nextEvent.eventMethod(nextEventDF, gsDF)
      nextEvent.joinMethod(res444, gsDF, joinOn).na.fill(0)
    })

    LOG.info(
      s"""
         |EventList:
         |${eventList.mkString(",")}
         |ReqEvents:
         |${reqEvents.mkString(",")}
      """.stripMargin)

    val useFields = rmMapNulls filter (gameSessionNoSchemaDF.columns contains)

    LOG.info(s"Replacing null maps with empty maps: ${useFields.mkString(", ")}")
    val updateGDDFTmp = DataFrameUtils.replaceNullsLong(gameSessionNoSchemaDF, useFields)

    //need to return updateGDDF schema to the original gameSessionDF in order to allow correct comparison later
//    val updateGDDF = updateGDDFTmp.sparkSession.createDataFrame(updateGDDFTmp.rdd, gameSessionNoSchemaDF.schema)

    val updateGDDF = updateGDDFTmp

    val rawPath = SparkUtils.createDtHrPath(s"${gsPathOut}_raw")(currentDtHr.head)
    LOG.info(s"Raw json path: $rawPath")

    updateGDDF.na.fill(0).coalesce(partitionNum).write.mode(SaveMode.Overwrite).json(rawPath)

    val gsSchema = DataFrameUtils.createStructTypeFromFile(s"$schemaPath/game_session.avsc")

    val fromJsonDF = sps.read.schema(gsSchema).json(rawPath)
    fromJsonDF.coalesce(partitionNum).write.mode(SaveMode.Overwrite).parquet(SparkUtils.createDtHrPath(gsPathOut)(currentDtHr.head))
    fromJsonDF
  }


  private def filterSids(sidTable: String, dt: String, hr: String, hoursBack: Int)(path: String)
                        (implicit sps: SparkSession) = {
    val ltNow = DateTimeUtils.getLocalDateTime(dt, hr)
    val ltBefore = ltNow.minusHours(hoursBack)

    val eventDF = SparkUtils.readDtDF(path, ltBefore.toLocalDate.toString, ltNow.toLocalDate.toString)

    val eventTable =
      SparkUtils
        .loadAndRegisterDF("event_table", eventDF, uniqueName = true)

    sps.sql(
      s"""
         |select *
         |from $eventTable
         |where
         |session_id in (
         | select * from $sidTable
         |)
      """.stripMargin)
  }

}



