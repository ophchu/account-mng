package com.dgn.core.utils

import java.io.{BufferedReader, InputStreamReader}

import org.apache.http.client.entity.UrlEncodedFormEntity

import scala.io.Source
import org.apache.http.client.methods.{HttpGet, HttpPost}
import org.apache.http.impl.client.{DefaultHttpClient, HttpClientBuilder}
import org.apache.http.message.BasicNameValuePair
import scala.collection.JavaConverters._

/**
  * Created by ophchu on 5/11/17.
  */
object HttpUtils {

  val client = HttpClientBuilder.create.build

  def getRequest(url: String) = {
    val res = scala.io.Source.fromURL(url)
    res.getLines().mkString("\n")
  }

  def postRequest(url: String, properties: Map[String, String]) = {

    val post = new HttpPost(url)
    val urlParams = properties.map(p => new BasicNameValuePair(p._1, p._2))
    post.setEntity(new UrlEncodedFormEntity(urlParams.asJava))

    val response = client.execute(post)
    val strRes = scala.io.Source.fromInputStream(response.getEntity.getContent)
    strRes.getLines().mkString("\n")
  }

//  def getRestContent(url:String): String = {
//    val httpClient = new DefaultHttpClient()
//    val httpResponse = httpClient.execute(new HttpGet(url))
//    val entity = httpResponse.getEntity()
//    var content = ""
//    if (entity != null) {
//      val inputStream = entity.getContent()
//      content = io.Source.fromInputStream(inputStream).getLines.mkString
//      inputStream.close
//    }
//    httpClient.getConnectionManager().shutdown()
//    return content
//  }
}
