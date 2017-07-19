import sbt._

object Dependencies {
  val resolutionRepos = Seq(
    "Maven Repository" at "http://mvnrepository.com/artifact/",
    "Job Server Bintray" at "http://dl.bintray.com/spark-jobserver/maven",
    "Spark Packages Repo" at "http://dl.bintray.com/spark-packages/maven"
  )


  object V {

    val commonsLang = "2.6"
    val scalaTest = "2.0.0"
    val slf4j = "1.7.7"

    val typesSagfeConfVer = "1.3.1"
    val avro = "1.7.6"

    val commons_io = "2.4"

    val commons_cli = "1.2"

    val junit = "4.11"
    val junit_interface = "0.11"
    val testng = "6.8.8"

    val sparkCore = "2.0.1"

  }

  object Libraries {

    // General toolset
    val commonsLang = "commons-lang" % "commons-lang" % V.commonsLang
    val scalaTest = "org.scalatest" % "scalatest_2.10" % V.scalaTest % "test"

    val avro = "org.apache.avro" % "avro" % V.avro
    val apache_commons_io = "commons-io" % "commons-io" % V.commons_io
    val apache_commons_net = "commons-net" % "commons-net" % "3.4"
    val jsch = "com.jcraft" % "jsch" % "0.1.53"

    val typeSafeConf = "com.typesafe" % "config" % "1.3.1"

    val slf4j = "org.slf4j" % "slf4j-log4j12" % V.slf4j

    val javax_servlet = "javax.servlet" % "javax.servlet-api" % "3.1.0"
    val commons_cli = "commons-cli" % "commons-cli" % "1.2"

    val junit = "junit" % "junit" % V.junit % "test"
    val junit_interface = "com.novocode" % "junit-interface" % V.junit_interface % "test"
    val testng = "org.testng" % "testng" % V.testng % "test"
    val findBugs = "com.google.code.findbugs" % "jsr305" % "3.0.0"
    val graphiteMetrics = "nl.grons" %% "metrics-scala" % "3.5.3_a2.3"



    val sparkCore = "org.apache.spark" % "spark-core_2.10" % V.sparkCore % "provided"


    val sparkSql = "org.apache.spark" % "spark-sql_2.10" % V.sparkCore
    val sparkRepl = "org.apache.spark" % "spark-repl_2.10" % V.sparkCore
    val sparkMllib = "org.apache.spark" % "spark-mllib_2.10" % V.sparkCore
    val sparkHive = "org.apache.spark" % "spark-hive_2.10" % V.sparkCore

    val sparkAvro = "com.databricks" %% "spark-avro" % "3.1.0"

    val hadoopClient = "org.apache.hadoop" % "hadoop-client" % "2.7.0"

    val mysqlConnector = "mysql" % "mysql-connector-java" % "5.1.34" % "test"
    val awsClient = "com.amazonaws" % "aws-java-sdk" % "1.9.37"
    val awsHadoop = "org.apache.hadoop" % "hadoop-aws" % "2.7.3"


    //    val jodaTime = "joda-time" % "joda-time" % "2.81"
  }

}
