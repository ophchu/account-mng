package com.dgn.core.utils

import org.slf4j.Logger

/**
  * Created by ophchu on 2/28/17.
  */
object LogUtils {

  def printValue(logger: Logger, msg: String, values: Any*) = {
    logger.info(
      s"""
        |$msg
        |
      """.stripMargin)
  }
}
