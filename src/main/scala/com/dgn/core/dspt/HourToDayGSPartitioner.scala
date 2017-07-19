package com.dgn.core.dspt

import com.dgn.core.utils.{FileUtils, SparkUtils}
import com.typesafe.config.ConfigFactory
import org.apache.spark.sql._
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.JavaConversions._
import scala.util.Try

/**
  * Created by ophchu on 2/5/17.
  */
object HourToDayGSPartitioner {
  private val LOG: Logger = LoggerFactory.getLogger(getClass)

  def repartition(confStr: String)
                 (implicit sps: SparkSession) = {
    import sps.implicits._
    val config = ConfigFactory.parseString(confStr)

    val dt = config.getString("dgn.htdp.dt")
    val dryrun = Try(config.getBoolean("dgn.htdp.dryrun")).getOrElse(false)

    val inPath = config.getString("dgn.htdp.paths.in")
    val outPath = config.getString("dgn.htdp.paths.out")
    val partitionNum = Try(config.getInt("dgn.htdp.parts")).getOrElse(10)


    LOG.info(
      s"""
        |repartition:
        |dt=$dt
        |dryrun=$dryrun
        |inPath=$inPath
        |outPath=$outPath
        |
      """.stripMargin)

      val inDF = SparkUtils.readDF(s"$inPath/dt=$dt")

      if (!dryrun) {
        SparkUtils.writeDF(inDF.coalesce(partitionNum), s"$outPath", dt)
      }
    inDF
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
