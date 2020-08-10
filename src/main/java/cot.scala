
import org.apache.spark.sql.SparkSession
import org.junit.Assert.assertEquals

class cot {
  val spark = SparkSession
    .builder()
    .appName("Spark SQL basic example")
    .master("local[3]")
    .getOrCreate()

  @org.junit.Test
  def test1(): Unit = {
    val expected: Long = 52
    val actual: Long = Kafka.countDf(spark, "C://Users//rajat//Desktop//lat_lon.csv")
    assertEquals(expected, actual)
  }
  @org.junit.Test
  def test2():Unit ={
    val dm= spark.read
      .option("header", "true")
      .csv("C://Users//rajat//Desktop//lat_lon.csv")
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
      .csv("C://Users//rajat//Desktop//weather_data.csv")
    val expext :Integer = 40000
val actual:Any= Kafka.allJoin(spark,"C://Users//rajat//Desktop//lat_lon.csv","C://Users//rajat//Desktop//weather_data.csv",2)

    assertEquals(actual,actual)

  }
}