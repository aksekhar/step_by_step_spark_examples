package dataframe

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{LongType, StringType, StructField, StructType,Metadata}

object structuredAPI extends App {
   val files = "/Volumes/DataHD/GIT_REPO/datasets_def/data/flight-data/json"
   val spark = SparkSession.builder().appName("structuredAPI").master("local")
     .getOrCreate()
   spark.sparkContext.setLogLevel("ERROR")
   val NumDF = spark.range(500).toDF("Num")
  NumDF.selectExpr("num","num+10").show(3)

   val df = spark.read.format("json").load(files)
  // println(df.schema)
   val flightSchema = StructType(Array(
                  StructField("DEST_COUNTRY_NAME",StringType,true),
                  StructField("ORIGIN_COUNTRY_NAME",StringType,true),
                  StructField("count",LongType,false,
                    Metadata.fromJson("{\"meta\":\"data\"}"))
                ))
  val dfOwnSchema = spark.read.format("json").schema(flightSchema).load(files)
  println(dfOwnSchema.schema)
  dfOwnSchema.printSchema()
  import  org.apache.spark.sql.functions.{col,column}
  val c1 = col("col1")
  val c2 = column("column2")
  df.show(1)
  df.first
  df.col("count")
import org.apache.spark.sql.Row
  val record = Row("Linku",1984,4443.00)
  println(record(0)+"\t"+record(0).asInstanceOf[String] )

}
