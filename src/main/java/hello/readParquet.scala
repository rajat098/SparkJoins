package hello

import org.apache.spark.sql.SparkSession

object readParquet extends App {
  val spark = SparkSession.builder.master("local").appName("spark session example").getOrCreate()
  spark.sparkContext.setLogLevel("ERROR")
  val data = spark.read.format("delta").load("E://data")
  data.show()
}
