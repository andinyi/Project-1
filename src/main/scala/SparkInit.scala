import org.apache.hadoop.fs.FileAlreadyExistsException
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql
import org.apache.spark.sql.{AnalysisException, DataFrame, Dataset, SaveMode, SparkSession}
import org.apache.spark

class SparkInit(appName:String) {
  val spark = SparkSession
    .builder
    .appName(appName)
    .config("spark.master", "local[*]")
    .enableHiveSupport()
    .getOrCreate()
  Logger.getLogger("org").setLevel(Level.OFF)
  Logger.getLogger("akka").setLevel(Level.OFF)

  def writeHDFS(df:DataFrame, path:String):Unit = {
    try {
      df.write.json(path)
    } catch {
      case e: AnalysisException => {
        println("File Already Exists! Overwriting...")
        df.write.mode(SaveMode.Overwrite).json(path)
      }
    }
  }

  def readHDFS(path:String):DataFrame = {
    val df = spark.read.json(path)
    df
  }

  def createRestaurant(name:String, cuisine:String):DataFrame = {
    import spark.implicits._
    val tmpList = Seq((name, cuisine))
    val rdd = spark.sparkContext.parallelize(tmpList)
    val df = rdd.toDF("dba", "cuisine_description")
    df
  }
}
