package com.dgn.core.utils

import java.io.File

import com.databricks.spark.avro.SchemaConverters
import org.apache.avro.Schema
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

import scala.collection.JavaConversions._
import scala.util.Random

/**
  * Created by ophchu on 1/2/17.
  */
object DataFrameUtils {
  val rnd = Random

  def createAvroSchema(avroSchemaStr: String) = {
    new Schema.Parser().parse(avroSchemaStr)
  }
  def createStructType(avroSchemaStr: String) = {
    val avroSchema = createAvroSchema(avroSchemaStr)
    SchemaConverters.toSqlType(avroSchema).dataType.asInstanceOf[StructType]

  }

  def createAvroSchemaFromFile(avroSchema: String) = {
    new Schema.Parser().parse(new File(avroSchema))
  }
  def createStructTypeFromFile(avroSchemaFile: String) = {
    val avroSchema = createAvroSchemaFromFile(avroSchemaFile)
    SchemaConverters.toSqlType(avroSchema).dataType.asInstanceOf[StructType]

  }

  def applyAvroSchema(avroSchemaFile: String, df: DataFrame) = {
    val structTypeMap = createStructTypeFromFile(avroSchemaFile).fields.map(sf => (sf.name, sf)).toMap
    val strcutFields = df.schema.fields.map(sf => structTypeMap(sf.name))
    df.sparkSession.createDataFrame(df.rdd, StructType(strcutFields))
  }

  case class ExceptCheck(
                          leftRegDF: DataFrame,
                          rightRegDF: DataFrame,
                          leftMapDF: Dataset[String],
                          rightMapDF: Dataset[String])

  case class ExceptCheckMaps(
                          leftRegDF: DataFrame,
                          rightRegDF: DataFrame,
                          intMapsDF: Option[(Dataset[String], Dataset[String])],
                          longMapsDF: Option[(Dataset[String], Dataset[String])])


  def compareDF3(df1: DataFrame, df2: DataFrame)
                (implicit sps: SparkSession) = {
    val mapIntCols = df1.schema.filter(_.dataType.toString == "MapType(StringType,IntegerType,true)").map(_.name)
    val mapLongCols = df1.schema.filter(_.dataType.toString == "MapType(StringType,LongType,true)").map(_.name)

    val noMapDf1 = df1.drop(mapIntCols: _*).drop(mapLongCols: _*)
    val noMapDf2 = df2.drop(mapIntCols: _*).drop(mapLongCols: _*)

    ExceptCheckMaps(
      noMapDf1.except(noMapDf2),
      noMapDf2.except(noMapDf1),
      compareIntMaps(df1.selectExpr(mapIntCols: _*), df2.selectExpr(mapIntCols: _*)),
      compareLongMaps(df1.selectExpr(mapLongCols: _*), df2.selectExpr(mapLongCols: _*))
    )
  }

  def compareDF2(df1: DataFrame, df2: DataFrame)
                (implicit sps: SparkSession) = {
    val mapCols = df1.schema.filter(_.dataType.toString == "MapType(StringType,IntegerType,true)").map(_.name)

    val noMapDf1 = df1.drop(mapCols: _*)
    val noMapDf2 = df2.drop(mapCols: _*)

    val mapDf1 = df1.selectExpr(mapCols: _*)
    val mapDf2 = df2.selectExpr(mapCols: _*)

    val mapCom = compareIntMaps(mapDf1, mapDf2)
    ExceptCheck(
      noMapDf1.except(noMapDf2),
      noMapDf2.except(noMapDf1),
      mapCom.get._1,
      mapCom.get._2
    )
  }

  def compareDF(df1: DataFrame, df2: DataFrame) = {
    (df1.except(df2), df2.except(df1))
  }

  def compareLongMaps(df1: DataFrame, df2: DataFrame)
                     (implicit sps: SparkSession) :Option[(Dataset[String], Dataset[String])]= {
    import sps.implicits._
    df1.schema.isEmpty match {
      case true => None
      case false =>
        val res1 = df1.map(row => {
          if (row.isNullAt(0)){
            "null"
          }else{
            row.getMap[String, Long](0).toList.sorted.mkString(",")
          }

        })
        val res2 = df2.map(row => {
          if (row.isNullAt(0)){
            "null"
          }else{
            row.getMap[String, Long](0).toList.sorted.mkString(",")
          }

        })
        Some(res1.except(res2), res2.except(res1))
    }
  }

  def compareIntMaps(df1: DataFrame, df2: DataFrame)
                     (implicit sps: SparkSession) :Option[(Dataset[String], Dataset[String])]= {
    import sps.implicits._
    df1.schema.isEmpty match {
      case true => None
      case false =>
        val res1 = df1.map(row => {
          if (row.isNullAt(0)){
            "null"
          }else{
            row.getMap[String, Int](0).toList.sorted.mkString(",")
          }

        })
        val res2 = df2.map(row => {
          if (row.isNullAt(0)){
            "null"
          }else{
            row.getMap[String, Int](0).toList.sorted.mkString(",")
          }

        })
        Some(res1.except(res2), res2.except(res1))
    }
  }


  def compareMaps(df1: DataFrame, df2: DataFrame, colName: String)
                 (implicit sps: SparkSession) = {
    import sps.implicits._
    val res1 = df1.select(colName).map(row => row.getMap[String, Int](0).toList.sorted.toString)
    val res2 = df2.select(colName).map(row => row.getMap[String, Int](0).toList.sorted.toString)
    (res1.except(res2), res2.except(res1))
  }

  def replaceNullsInt(df: DataFrame, mapsColList: List[String])
                   (implicit sps: SparkSession): DataFrame = {
    import org.apache.spark.sql.functions._
    mapsColList.foldLeft(df)((df1, colName) => df1.withColumn(colName, reNullsInt(col(colName))))
  }

  def replaceNullsLong(df: DataFrame, mapsColList: List[String])
                     (implicit sps: SparkSession): DataFrame = {
    import org.apache.spark.sql.functions._
    mapsColList.foldLeft(df)((df1, colName) => df1.withColumn(colName, reNullsLong(col(colName))))
  }


  def removeNullsLong(input: Map[String, Long]): Map[String, Long] = {
    input match {
      case null => Map.empty[String, Long]
      case _ => input
    }
  }

  def removeNullsInt(input: Map[String, Int]): Map[String, Int] = {
    input match {
      case null => Map.empty[String, Int]
      case _ => input
    }
  }

  val reNullsLong = udf(removeNullsLong _)
  val reNullsInt = udf(removeNullsInt _)


  def printStruct(struct: Row) = {
    struct.toString()
  }


  def printDF(df: DataFrame): Unit = {
    df.printSchema()
    println()
    df.collect().foreach(println)
  }
}
