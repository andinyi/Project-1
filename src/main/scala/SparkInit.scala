import org.apache.hadoop.fs.FileAlreadyExistsException
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql
import org.apache.spark.sql.{AnalysisException, DataFrame, Dataset, SparkSession}

class SparkInit(appName:String) {
  val spark = SparkSession
    .builder
    .appName(appName)
    .config("spark.master", "local[*]")
    .enableHiveSupport()
    .getOrCreate()
  Logger.getLogger("org").setLevel(Level.ERROR)

  def writeHDFS(df:DataFrame):Unit = {
    try {
      df.write.json("hdfs://localhost:9000/tmp/project1/restaurantInspectionJson.json")
    } catch {
      case e: AnalysisException => println("File Already Exists!")
    }
  }

  def readHDFS():DataFrame = {
    val df = spark.read.json("hdfs://localhost:9000/tmp/project1/restaurantInspectionJson.json")
    df
  }


}
