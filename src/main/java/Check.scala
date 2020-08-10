
import org.apache.spark.sql._

object Kafka extends App {
  val spark = SparkSession
    .builder()
    .appName("Spark SQL basic example")
    .master("local[3]")
    .getOrCreate()

  val df3 = countDf(spark, "C://Users//rajat//Desktop//lat_lon.csv")

  def countDf(spark: SparkSession, dataFile22: String): Long = {
    spark.read
      .option("header", "true")
      .csv(dataFile22)
      .createOrReplaceTempView("lat_long_vw")
    var asset_id_1 = ("575850", "575839", "575854", "575834", "575849", "575846", "575851", "575843", "575835", "575845", "575853", "575833",
      "575848")
    var asset_id_2 = (575836, 575842, 575841, 575852, 575844, 575838, 575856, 575855, 575857, 575847, 575840, 575837)
    var query_1 = s"SELECT _lat ,_lon FROM lat_long_vw where asset_id in $asset_id_1"
    var query_2 = s"SELECT _lat ,_lon FROM lat_long_vw where asset_id in $asset_id_2"
    val dd = spark.sql(query_1) //.show()
    val dd2 = spark.sql(query_2)
    dd.unionByName(dd2)
    dd.count()
  }

  def allJoin(session: SparkSession, dataFile1: String, dataFile2: String, dig:Int):Any = {
   val dm= spark.read
      .option("header", "true")
      .csv(dataFile1)
     dm.createOrReplaceTempView("lat_long_vw")
    var asset_id_1 = ("575850", "575839", "575854", "575834", "575849", "575846", "575851", "575843", "575835", "575845", "575853", "575833",
      "575848")
    var asset_id_2 = (575836, 575842, 575841, 575852, 575844, 575838, 575856, 575855, 575857, 575847, 575840, 575837)
    var query_1 = s"SELECT _lat ,_lon FROM lat_long_vw where asset_id in $asset_id_1"
    var query_2 = s"SELECT _lat ,_lon FROM lat_long_vw where asset_id in $asset_id_2"
    val dd = spark.sql(query_1) //.show()
    val dd2 = spark.sql(query_2)
    val final1 = dd.unionByName(dd2)
    val final2 = spark.read
      .option("header", "true")
      .csv(dataFile2)
    if (dig==1) {
      val fin = final2.join(final1, final2("lat") === final1("_lat") or final2("lon") === final1("_lon"), "inner")
      fin.show(5)
  fin.count()
    }
    else if(dig==2){
      val fin2 = final2.join(final1, final2("lat") === final1("_lat") or final2("lon") === final1("_lon"), "left")
      fin2.show(5)
       fin2.count()
    }
    else if(dig==3){
      val fin3 = final2.join(final1, final2("lat") === final1("_lat") or final2("lon") === final1("_lon"), "leftanti")
      fin3.show(5)
      fin3.count()
    }
    else if(dig==4){
      val fin4 = final2.join(final1, final2("lat") === final1("_lat") or final2("lon") === final1("_lon"), "right")
      fin4.show(5)
     fin4.count()
    }
  }:Any
 val checking = allJoin(spark, "C://Users//rajat//Desktop//lat_lon.csv", "C://Users//rajat//Desktop//weather_data.csv",2)
print(checking)
}


