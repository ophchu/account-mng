package com.dgn.core.utils

import com.amazonaws.auth.BasicAWSCredentials
import com.amazonaws.services.cloudwatch.AmazonCloudWatchAsyncClient
import com.amazonaws.services.cloudwatch.model.{Dimension, MetricDatum, PutMetricDataRequest, StandardUnit}

import scala.collection.mutable

/**
  * Created by ophchu on 5/9/17.
  */
object CloudWatchUtils {
  val accessKey = "AKIAIBUYGVMDO7C6Y2MQ"
  val secretKey = "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"

  val cw = new AmazonCloudWatchAsyncClient(new BasicAWSCredentials(accessKey, secretKey))

  val dimensions = List(
    new Dimension().withName("env").withValue("core"),
    new Dimension().withName("product").withValue("lts")
  )
  val namespace = "dgn/dbc"

  val timers = mutable.Map.empty[String, Long]

  def startMeasureTime(name: String) = timers.put(name, System.currentTimeMillis())
  def endAndSendMeasure(name: String) = {
    val jobTime = System.currentTimeMillis - timers(name)
    sendJobTime(name, jobTime)
    timers.remove(name)
  }

  def sendJobTime(name: String, jobRunTime: Long) = {
    sendMetrics(s"""${name}_runtime""", jobRunTime, "Milliseconds")
  }

  def sendJobStatus(name: String, success: Boolean) = {
    val value = success match {
      case true => 1.0
      case false => 0.0
    }
    sendMetrics(s"""${name}_status""", value, "None")
  }

  def sendMetrics(name: String, value: Double, unit: String) = {
    val datum = new MetricDatum()
      .withMetricName(name)
      .withUnit(unit)
      .withValue(value)
      .withDimensions(dimensions: _*)

    val request = new PutMetricDataRequest()
      .withNamespace("dgn/dbc")
      .withMetricData(datum)

    cw.putMetricData(request)
  }


}
