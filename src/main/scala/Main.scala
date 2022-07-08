import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql

object Main {
  def main(args: Array[String]): Unit = {
    val json = new apiJsonGet("https://data.cityofnewyork.us/resource/43nn-pn8j.json")
    json.getJson()
    val session = new SparkInit("Project 1")
    var df = session.spark.read.json("tmp.json")
    session.writeHDFS(df)
    df = session.readHDFS()

    //Query 1
    df.createOrReplaceTempView("query1") //All Restaurants with Rating B and Above
    session.spark.sql("SELECT dba as Name, cuisine_description as Cuisine, grade as Grade FROM query1 WHERE Grade = 'B' OR Grade = 'A'").show()
  }
}