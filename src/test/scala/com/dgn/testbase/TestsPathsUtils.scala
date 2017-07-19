package com.dgn.testbase

import java.io.{File, IOException}
import java.nio.file.attribute.BasicFileAttributes
import java.nio.file._

import org.slf4j.LoggerFactory

/**
  * Created by ophchu on 1/1/17.
  */
object TestsPathsUtils {
  private val LOG = LoggerFactory.getLogger(getClass)

  def getAbsoultePath(path: String) = new File(path).getAbsolutePath

  def getLtsInPath(eventPath: String) = new File(getAbsoultePath("src/test/resources/in/lts"), eventPath).getAbsolutePath
  def getOvsInPath(eventPath: String) = new File(getAbsoultePath("src/test/resources/in/ovs"), eventPath).getAbsolutePath

  def getLtsOutPath(eventPath: String) = new File(getAbsoultePath("src/test/resources/out/lts"), eventPath).getAbsolutePath
  def getOvsOutPath(eventPath: String) = new File(getAbsoultePath("src/test/resources/out/ovs"), eventPath).getAbsolutePath


  def deleteLtsOut() = deletePath(getLtsOutPath("/"))
  def deleteOvsOut() = deletePath(getOvsOutPath("/"))


  private def deletePath(pathStr: String) = {
    LOG.info(s"Going to delete $pathStr")
    val root = Paths.get(pathStr)

    Files.exists(root) match {
      case true => {
        var filesCount = 0
        var dirCount = 0
        Files.walkFileTree(root, new SimpleFileVisitor[Path]{
          override def visitFile(file: Path, attrs: BasicFileAttributes): FileVisitResult = {
            Files.delete(file)
            filesCount+=1
            FileVisitResult.CONTINUE
          }

          override def postVisitDirectory(dir: Path, exc: IOException): FileVisitResult = {
            Files.delete(dir)
            dirCount+1
            FileVisitResult.CONTINUE
          }
        })
        LOG.info(s"Deleted $filesCount files in $dirCount dirs")
      }
      case false =>
        LOG.info(s"Path $pathStr does not exists")
    }


  }


//  def getLtsInEventPath(eventPath: String) = new File(getAbsoultePath("src/test/resources/in/lts/event"), eventPath).getAbsolutePath
//  def getLtsInRawEventPath(eventPath: String) = new File(getAbsoultePath("src/test/resources/in/lts/raw/event"), eventPath).getAbsolutePath
//  def getLtsInReportEventPath(eventPath: String) = new File(getAbsoultePath("src/test/resources/in/lts/report"), eventPath).getAbsolutePath
//
//  def getLtsOutEventPath(eventPath: String) = new File(getAbsoultePath("src/test/resources/out/lts/event"), eventPath).getAbsolutePath
//  def getLtsOutRawEventPath(eventPath: String) = new File(getAbsoultePath("src/test/resources/out/lts/raw/event"), eventPath).getAbsolutePath
//  def getLtsOutReportPath(eventPath: String) = new File(getAbsoultePath("src/test/resources/out/lts/report"), eventPath).getAbsolutePath

}
