package dataframe

import org.apache.spark.sql.SparkSession

object dataframeAndSQL extends App {

  val path = "/Volumes/DataHD/GIT_REPO/Spark-The-Definitive-Guide/data"
  val flightFile = path+"/flight-data/csv"

  val spark = SparkSession.builder().appName("DataFrameSQL").master("local").getOrCreate()
  spark.sparkContext.setLogLevel("ERROR")
  val flightDF = spark.read.option("header","true").option("inferSchema","true")
    .csv(flightFile)
  flightDF.show(3)
  flightDF.take(2).foreach(println)
  flightDF.printSchema()
  // By default, when we perform a shuffle, Spark outputs 200 shuffle partitions
  flightDF.sort("count").explain()
  spark.conf.set("spark.sql.shuffle.partitions", "5")
  flightDF.sort("count").explain()
  flightDF.count()

  val grpDF = flightDF.groupBy("DEST_COUNTRY_NAME").count()
  grpDF.count()
  grpDF.show()

  //In SQL Way
  flightDF.createOrReplaceTempView("flight_vw")
  val grpVw = spark.sql(
    """select DEST_COUNTRY_NAME,count(1)
      |from flight_vw
      |group by DEST_COUNTRY_NAME
      |""".stripMargin)

  grpVw.show()
  //verify the explain plan, Both are same
  grpDF.explain()
  grpVw.explain()

   import org.apache.spark.sql.functions.{max,desc}
   flightDF.select(max("count")).show()
  spark.sql(
    """
      |select DEST_COUNTRY_NAME,sum(count) destination_total
      |from flight_vw
      |group by DEST_COUNTRY_NAME
      |order by sum(count) desc
      |limit 3
      |""".stripMargin).show()
  // same thing if you want write in DataFrame syntax little different the ordering
  val aggData = flightDF.groupBy("DEST_COUNTRY_NAME").sum( "count")
    .withColumnRenamed("sum(count)","destination_total")
    .sort(desc("destination_total"))
    .limit(3)
   aggData.show()
   aggData.explain()

    aggData.write.format("orc").partitionBy("DEST_COUNTRY_NAME")
    .save("/Volumes/DataHD/GIT_REPO/dataset/output/flightDataAggPaart")
   aggData.write.format("orc")
    .bucketBy(3,"destination_total")
   .saveAsTable("bucketflight")



}
