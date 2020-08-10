package hello
import org.apache.spark.sql._

object new1 extends App {
  val spark = SparkSession
    .builder()
    .appName("Spark SQL basic example")
    .master("local[3]")
    .getOrCreate()

  val df = spark.read.format("csv")
    .option("header", "true")
    .load("C://Users//rajat//Desktop//lat_lon.csv")
  df.createOrReplaceTempView("lat_long_vw")
  var asset_id_1 = ("575850", "575839", "575854", "575834", "575849", "575846", "575851", "575843", "575835", "575845", "575853", "575833",
    "575848")
  var asset_id_2 = (575836, 575842, 575841, 575852, 575844, 575838, 575856, 575855, 575857, 575847, 575840, 575837)

  var query_1 = s"SELECT _lat ,_lon FROM lat_long_vw where asset_id in $asset_id_1"
  var query_2 = s"SELECT _lat ,_lon FROM lat_long_vw where asset_id in $asset_id_2"
  //  print(query_1)
  val df1 = spark.sql(query_1) //.show()
  val df2 = spark.sql(query_2)
  val final_df_lat_long = df1.unionByName(df2)
  //final_df_lat_long.show()

  val weather_date_df = spark.read.format("csv")
    .option("header", "true")
    .load("C://Users//rajat//Desktop//weather_data.csv")

  weather_date_df.createOrReplaceTempView("weather_data_vw")
  //spark.sql("select * from weather_data_vw where lat = '23.8' or lon = '71.5'").show()
 val final_df_weather_data = weather_date_df.join(final_df_lat_long, weather_date_df("lat") === final_df_lat_long("_lat") or weather_date_df("lon") === final_df_lat_long("_lon"), "inner")
  //final_df_weather_data.show()44
// var new22: Long = final_df_weather_data.count()
 // print(new22)
  //***************----JOINS------*********************//
  val left_join =weather_date_df.join(final_df_lat_long,weather_date_df("lat")===df("_lat") or weather_date_df("lon")=== df("_lon"), "left")
val right_join =weather_date_df.join(final_df_lat_long,weather_date_df("lat")===df("_lat") or weather_date_df("lon")=== df("_lon"), "right")
  val left_anti_join =weather_date_df.join(final_df_lat_long,weather_date_df("lat")===df("_lat") or weather_date_df("lon")=== df("_lon"), "leftanti")
//  val right_anti_join =weather_date_df.join(final_df_lat_long,weather_date_df("lat")===df("_lat") or weather_date_df("lon")=== df("_lon"), "rightanti")
//  print("left_join")
//  left_join.show()
//    print("right_join")
//    right_join.show()
    print("left_anti_join")
    left_anti_join.show()
//    print("right_anti_join")
//    right_anti_join.show()
//  def count_result(vvv: DataFrame):Long= {
//     vvv.count()
//    if vvv == "left_join":
//     scala v = left_join()
//    return v.count()
//  }
  //print(count_result(final_df_weather_data))
  //final_df_weather_data.write.format("delta").save("E://data")
 // final_df_weather_data.write.format("delta").partitionBy("lat","lon").save("E://data//dir")


  //  |23.8|  71.5|
  //  |23.8|71.375|"
  //  |23.6|  71.5|
  //  |23.6|71.375|
  //  |23.6|  71.5|
  //  |23.6|71.375|
  //  |23.8|  71.5|
}


