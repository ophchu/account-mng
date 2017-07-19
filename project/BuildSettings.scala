import sbt.Keys._
import sbt._

object BuildSettings {

  // Basic settings for our app
  lazy val basicSettings = Seq[Setting[_]](
    name := "account-mng",
    organization := "ophchu",
    //Version is coming from version.sbt in the root dir where it update by sbt release plugin.
    //    version := s"$currentVersion",
    description := "account-mng",

    scalaVersion := "2.10.6",
    scalaVersion in ThisBuild := "2.10.6",
    scalacOptions := Seq("-deprecation", "-encoding", "utf8"),
    scalacOptions ++= Seq("-Xmax-classfile-name","78"),
    parallelExecution in Test := false,
    compileOrder := CompileOrder.JavaThenScala,
    resolvers ++= Dependencies.resolutionRepos,
    fork := true,
    fork in Test := true,
//    javaOptions in Test ++= Seq("-Xmx4G")
//        javaOptions in Test += "-Xmx4G -XX:PermSize=512m  -XX:MaxPermSize=1028m -XX:+CMSClassUnloadingEnabled -XX:+CMSPermGenSweepingEnabled -XX:+UseConcMarkSweepGC"
        javaOptions in Test += "-Xmx4G"

  )

  lazy val buildSettings = basicSettings
}
