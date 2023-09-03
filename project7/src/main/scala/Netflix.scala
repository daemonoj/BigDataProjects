import org.apache.spark._
import org.apache.spark.sql._

object Netflix {

  def main ( args: Array[ String ]): Unit = {
    val conf = new SparkConf().setAppName("Netflix")
    val sc = new SparkContext(conf)
    val spark = SparkSession.builder().config(conf).getOrCreate()
    import spark.implicits._

    /* ... */
    // val netflixDFCsv = spark.read.format("csv")
    //         .load(args(0))
    // netflixDFCsv.show()

    val data = sc.textFile(args(0))
    val csvToDF  = data.filter(x =>   !(x.contains(":")))
    // csvToDF.foreach(println)
    .map(f => {val a = f.split(",")
    (a(0).toInt, a(1).toInt) }).toDF("user","ratings")
    // csvToDF.show()
    csvToDF.registerTempTable("UserRatings")
    val sql_con = spark.sqlContext
    val averageRatings = sql_con.sql("SELECT AVG_RATING/10 as RATINGS, COUNT(AVG_RATING) as COUNT_AVG from (SELECT user, CAST((SUM(ratings)/COUNT(user))*10 as INT) as AVG_RATING from UserRatings GROUP BY user) GROUP BY RATINGS ORDER BY RATINGS ASC")
    averageRatings.show(100)
    sc.stop()

   
  }
}
