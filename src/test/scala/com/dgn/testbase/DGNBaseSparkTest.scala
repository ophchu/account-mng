package com.dgn.testbase

import java.nio.file.{Files, Path, Paths}

import com.dgn.spark.utils.SparkTestUtils
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach, FunSuite, Matchers}

/**
  * Created by ophchu on 11/24/16.
  */
class DGNBaseSparkTest extends DGNBaseTest{
  lazy implicit val sps = SparkTestUtils.createSparkSession()

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    TestsPathsUtils.deleteLtsOut
  }
}
