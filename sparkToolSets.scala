package dataframe

import org.apache.spark.sql.SparkSession

object sparkToolSets extends App{
  val path = "/Volumes/DataHD/GIT_REPO/datasets_def/data"
  val flightFile = path+"/flight-data/csv"

  val spark = SparkSession.builder().appName("ToolSets").master("local")
    .getOrCreate()
  case class Flight(DEST_COUNTRY_NAME: String,
                    ORIGIN_COUNTRY_NAME: String,
                    count: Int
                   )

  val flightDf = spark.read.option("header","true").option("inferSchema","true").
    csv(flightFile)
   import spark.implicits._
 val flightDs = flightDf.as[Flight]
  flightDs.show(3)
  flightDf.printSchema();flightDs.printSchema()

  flightDs.filter(f => f.ORIGIN_COUNTRY_NAME !="Canada")
    .map(f => f)
    .show(5)
  /*
   If do remove any column from DataSet its become DataFrame see below examples
   */
  flightDs.filter(f => f.ORIGIN_COUNTRY_NAME !="Canada")
    .map(f => (f.ORIGIN_COUNTRY_NAME,f.DEST_COUNTRY_NAME))
    .show(5)
  val retailDF = spark.read.format("csv").option("header","true").option("inferSchema","true")
    .load("/Volumes/DataHD/GIT_REPO/datasets_def/data/retail-data/by-day")
  retailDF.createOrReplaceTempView("vw_retail")
  spark.sql("select * from vw_retail").show(3)
  val rtlSchema = retailDF.schema
  import org.apache.spark.sql.functions.{window, column, desc, col}
  val purchaseByCustomerPerHour = retailDF.selectExpr("CustomerId",
    "(UnitPrice*Quantity) as total_cost",
    "InvoiceDate").groupBy(col("CustomerID"),window(col("InvoiceDate"),"1 day")
   ).sum("total_cost")

 /* purchaseByCustomerPerHour.writeStream.format("console")
    .queryName("vw_perHour_purchase")
    .outputMode("complete")
    .start()
  spark.sql("""
  SELECT *
  FROM customer_purchases
  ORDER BY `sum(total_cost)` DESC
  """)
    .show(5)*/
   val textRDD = spark.sparkContext.textFile("/Volumes/DataHD/GIT_REPO/datasets_def/data/README.md")
     textRDD.flatMap(word => word.split(" ")).map(x =>(x,1)).reduceByKey(_+_).foreach(println)

}
