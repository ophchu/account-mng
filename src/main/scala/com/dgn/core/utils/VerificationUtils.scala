package com.dgn.core.utils

import java.nio.file._
import java.nio.file.attribute.BasicFileAttributes
import java.util.regex.Pattern

import org.apache.spark.sql.SparkSession

import scala.collection.mutable

/**
  * Created by ophchu on 3/8/17.
  */
object VerificationUtils {

  case class ListRegexDirsVisitor(regex: String) extends SimpleFileVisitor[Path] {
    val fileList = mutable.MutableList.empty[Path]

    val pattern = Pattern.compile(regex)

    override def preVisitDirectory(dir: Path, attrs: BasicFileAttributes): FileVisitResult = {
      if (pattern.matcher(dir.toAbsolutePath.toString).matches) {
        fileList += dir
      }
      FileVisitResult.CONTINUE
    }
  }

  case class DirsConfig(event: String, dt: String, path: String)

  def findDupSpark(root:String, dirList: List[String])
                  (implicit sps: SparkSession) = {
    import sps.implicits._
    val dtList = DateTimeUtils.getLocalDateDaysBack(DateTimeUtils.getCurrentDateTime().toLocalDate.toString, 31)
    val dirsDS = dirList.flatMap(event =>
      dtList.map(dts => DirsConfig(event, dts.toString, s"$root/$event/dt=$dts"))

    ).toDS

    val res = dirsDS.flatMap(dir =>
      findDups(dir.path)
    )
  }

  def findDups(root: String) = {
    val patternStr = ".*\\/dt=\\d{4}-\\d{2}-\\d{2}\\/hr=\\d{2}\\/hr=\\d{2}"
    findTheXXX(root, patternStr)

  }

  def findTheXXX(root: String, regex: String) = {
    val rootPath = Paths.get(root)
    val lrdv = ListRegexDirsVisitor(regex)
    val res = Files.walkFileTree(rootPath, lrdv)
    lrdv.fileList.map(_.toString)
  }
}

