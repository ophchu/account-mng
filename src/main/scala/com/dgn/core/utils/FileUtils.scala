package com.dgn.core.utils

import java.io.File
import java.nio.file.attribute.BasicFileAttributes
import java.nio.file._
import java.util.regex.Pattern

import scala.collection.JavaConversions._

/**
  * Created by ophchu on 1/17/17.
  */
object FileUtils {


  def ls(pathStr: String) = {
    val res = Files.list(Paths.get(pathStr))
    res.iterator().toList
  }
}
