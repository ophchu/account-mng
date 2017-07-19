package com.dgn.core.utils

import java.util.regex.Pattern

import com.dgn.testbase.{DGNBaseSparkTest, DGNBaseTest, TestsPathsUtils}

/**
  * Created by ophchu on 3/8/17.
  */
class VerificationUtils$Test extends DGNBaseTest {

  test("testFindTheXXX") {
    val findIt = VerificationUtils.findTheXXX(s"${TestsPathsUtils.getLtsInPath("event/")}", "dt=2016-12-23")
    println()
  }

  test("findDups") {
    val findIt = VerificationUtils.findDups(s"${TestsPathsUtils.getLtsInPath("event/")}")
    println(findIt.mkString("\n"))
  }
}
