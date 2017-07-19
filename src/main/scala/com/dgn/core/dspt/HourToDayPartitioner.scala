package com.dgn.core.dspt

import com.dgn.core.utils.{FileUtils, SparkUtils}
import com.typesafe.config.ConfigFactory
import org.apache.spark.sql._
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.JavaConversions._
import scala.util.{Failure, Success, Try}

/**
  * Created by ophchu on 2/5/17.
  */
object HourToDayPartitioner {
  private val LOG: Logger = LoggerFactory.getLogger(getClass)

  case class H2DStatus(path: String, event: String, dt: String, count: Long)

  def repartition(confStr: String)
                 (implicit sps: SparkSession): Dataset[H2DStatus] = {
    import sps.implicits._
    val config = ConfigFactory.parseString(confStr)

    val dt = config.getString("dgn.htdp.dt")
    val dryrun = Try(config.getBoolean("dgn.htdp.dryrun")).getOrElse(false)

    val inPath = config.getString("dgn.htdp.paths.in")
    val outPath = config.getString("dgn.htdp.paths.out")
    val partitionNum = Try(config.getInt("dgn.htdp.parts")).getOrElse(15)


    LOG.info(
      s"""
        |repartition:
        |dt=$dt
        |dryrun=$dryrun
        |inPath=$inPath
        |outPath=$outPath
        |
      """.stripMargin)

    val eventList =
      Try(config.getStringList("dgn.htdp.events.in").toList).getOrElse{
        FileUtils.ls(s"/dbfs/$inPath").map(_.getFileName.toString)
      }

    LOG.info(
      s"""
        |EventList:
        |${eventList.mkString("\n")}
      """.stripMargin)

    eventList.map(event => {
      Try(SparkUtils.readDF(s"$inPath/$event/dt=$dt")) match {
        case Success(inDF) =>
          SparkUtils.writeDF(inDF.coalesce(partitionNum), s"$outPath/$event", dt)
          H2DStatus(s"$inPath/$event/dt=$dt", event, dt, inDF.count)

        case Failure(ex) =>
          LOG.info(s"Failed to read $event")
          H2DStatus(s"$inPath/$event/dt=$dt", event, dt, -1)
      }
    }).toDS
  }

  val htdpConf =
    """
      |dgn {
      |  htdp {
      |    dt = "2016-12-23"
      |    dryrun = false
      |
      |    paths {
      |      in = ${PathsUtils.getAbsoultePath("/dbfs/mnt/ltsprodbackup/lts/raw/event/")}
      |      out = ${PathsUtils.getAbsoultePath("src/test/resources/out/game_session")}
      |      dbc_prefix = "/dbc"
      |    }
      |
      |    events{
      |      in = [start_session, spin, end_session]
      |    }
      |
      |  }
      |}
      |
    """.stripMargin
}
