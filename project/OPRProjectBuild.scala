import Dependencies.Libraries
import BuildSettings.buildSettings
import sbt.Keys._
import sbt._

object OPRProjectBuild extends Build {

  // Configure prompt to show current project
  override lazy val settings = super.settings :+ {
    shellPrompt := { s => Project.extract(s).currentProject.id + " > " }
  }

  // Define our project, with basic project information and library dependencies

  // OC 20130710 - disabled checksums to get paralllelai.spyglass 2.0.4 jar to download.  
  // Probably worth removing with future versions.
  lazy val project = Project("dgn-spark-dbc", file("."))
    .settings(net.virtualvoid.sbt.graph.Plugin.graphSettings: _*)
    .settings(buildSettings: _*)
    .settings(checksums in update := Nil)
    .settings(conflictWarning := ConflictWarning.disable)
    .settings(
      libraryDependencies ++= Seq(

        // Utils
        Libraries.commonsLang,
        Libraries.scalaTest,
        Libraries.apache_commons_io,
        Libraries.apache_commons_net,
        Libraries.jsch,
        Libraries.slf4j,
        Libraries.javax_servlet,
        Libraries.commons_cli,
        Libraries.junit,
        Libraries.junit_interface,
        Libraries.testng,
        Libraries.typeSafeConf,

        // Hadoop
        Libraries.hadoopClient,

        // Spark
        Libraries.sparkCore,
        Libraries.sparkSql,
        Libraries.sparkRepl,
        Libraries.sparkMllib,
        Libraries.sparkHive,

        Libraries.sparkAvro,

        Libraries.mysqlConnector,
        Libraries.awsHadoop,
        //        Libraries.jodaTime,

        Libraries.graphiteMetrics,
        Libraries.awsClient

      )
    )
}
