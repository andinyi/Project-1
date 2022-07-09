import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql
import Console.{GREEN, RED, RESET, UNDERLINED, YELLOW, CYAN}
import scala.io.StdIn.{readInt, readLine}

object Main {
  def main(args: Array[String]): Unit = {
    val inspectJson = new apiJsonGet("https://data.cityofnewyork.us/resource/43nn-pn8j.json")
    inspectJson.getJson("InspectionJson.json")
    val openSeatingJson = new apiJsonGet("https://data.cityofnewyork.us/resource/4dx7-axux.json")
    openSeatingJson.getJson("openSeatingJson.json")
    val session = new SparkInit("Project 1")
    var df = session.spark.read.json("InspectionJson.json")
    var df2 = session.spark.read.json("openSeatingJson.json")
    session.writeHDFS(df, "hdfs://localhost:9000/tmp/project1/restaurantInspectionJson.json")
    session.writeHDFS(df2, "hdfs://localhost:9000/tmp/project1/openSeatingJson.json")
    df = session.readHDFS("hdfs://localhost:9000/tmp/project1/restaurantInspectionJson.json").dropDuplicates()
    df2 = session.readHDFS("hdfs://localhost:9000/tmp/project1/openSeatingJson.json").dropDuplicates()

    val printing = new UserPrompt
    var userInput: String = ""
    var resultNum: Int = 100
    val choices: List[String] = List("1", "2", "exit", "setRows")
    printing.printStart()
    val queryText = new QueryText
    while (true)
      {
        do {
          try {
            printing.printMenu()
            userInput = readLine()
          } //catch not in list()
          if(userInput == "exit") {
            return true
          }
          if (userInput == "1") { //Query 1: All Restaurants with Rating B and Above
            df.createOrReplaceTempView("query1") //All Restaurants with Rating B and Above
            session.spark.sql(queryText.getQuery1()).show(resultNum, false)
          }
          if (userInput == "2") { //Query 2: What are the restaurants that have been labeled critical (hence you should avoid them)
            df.createOrReplaceTempView("query2")
            session.spark.sql(queryText.getQuery2()).show(resultNum, false)
          }
          if (userInput == "3") { //Query 3: Which of the restaurants that have an Inspection Grade C or Above also provides outdoor seating because of Covid?
            df.createOrReplaceTempView("query3i")
            df2.createOrReplaceTempView("query3s")
            session.spark.sql(queryText.getQuery3()).show(resultNum, false)
          }
          if (userInput == "4") { //Query 4: What are the restaurants located in a borough, based on cuisine type
            df.createOrReplaceTempView("query4")
            var tmpInput:String = ""
            var cuisineType:String = ""
            val borough : List[String] = List("Brooklyn", "Manhattan", "Queens", "Bronx", "Staten Island")
            do {
              Console.println(s"${YELLOW}Enter your choice of borough!${RESET}")
              tmpInput = readLine().replace(" ", "")
              println(s"${YELLOW}Enter the type of cuisine!${RESET}")
              cuisineType = readLine()
            } while(!borough.contains(tmpInput))
            session.spark.sql(queryText.getQuery4(tmpInput, cuisineType)).show(resultNum, false)
          }
          if (userInput == "5") { //For the restaurant (x) what are its inspection grade, violation code, and violation description?
            df.createOrReplaceTempView("query5")
            var tmpInput:String = ""
            Console.println(s"${YELLOW}Enter restaurant name!${RESET}")
            tmpInput = readLine().toUpperCase()
            session.spark.sql(queryText.getQuery5(tmpInput)).show(resultNum, false)
          }
          if (userInput == "6") { //What is the average inspection grade based on zipcode?
            df.createOrReplaceTempView("query6")
            println(s"${CYAN}The average is represented numerically! Key of 1-4.${RESET}")
            session.spark.sql(queryText.getQuery6()).show(resultNum, false)
          }
          if (userInput == "setRows") {
            printing.printSetResults()
            try {
              resultNum = readInt()
            } //catch wrong type
          }
        } while (!choices.contains(userInput))
      }
  }
}