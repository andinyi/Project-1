import org.apache.hadoop.fs.FileAlreadyExistsException
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql
import org.apache.spark.sql.{AnalysisException, DataFrame, Dataset, SaveMode, SparkSession}

class SparkInit(appName:String) {
  val spark = SparkSession
    .builder
    .appName(appName)
    .config("spark.master", "local[*]")
    .enableHiveSupport()
    .getOrCreate()
  Logger.getLogger("org").setLevel(Level.ERROR)

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


}
