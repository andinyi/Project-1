import org.apache.log4j.{Level, Logger}
import org.apache.spark
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql

import java.io.File
import org.apache.spark.sql.functions._

import Console.{CYAN, GREEN, RED, RESET, UNDERLINED, YELLOW}
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
    printing.printStart()
    val queryText = new QueryText
    val connect = new MySQLConnect
    while (true) {
      println(s"(1) ${YELLOW}Login into Andy's Restaurant Data Emporium${RESET}")
      println(s"(2) ${YELLOW}Create a new account${RESET}")
      val choice = readLine()
      if (choice == "1") {
        println(s"${YELLOW}Please login. Enter your username!${RESET}")
        val user = readLine()
        println(s"${YELLOW}Please login. Enter your password!${RESET}")
        val password = readLine()
        if (connect.authMatch(user, password)) {
          val admin = connect.admin(user)
          do {
            try {
              if (admin) {
                printing.printAdminMenu()
              } else {
                printing.printUserMenu()
              }
              userInput = readLine()
            } //catch not in list()
            if (userInput == "exit") {
              df.write.mode(SaveMode.Overwrite).json("result_df")
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
              var tmpInput: String = ""
              var cuisineType: String = ""
              val borough: List[String] = List("Brooklyn", "Manhattan", "Queens", "Bronx", "Staten Island")
              do {
                Console.println(s"${YELLOW}Enter your choice of borough!${RESET}")
                tmpInput = readLine().replace(" ", "")
                println(s"${YELLOW}Enter the type of cuisine!${RESET}")
                cuisineType = readLine()
              } while (!borough.contains(tmpInput))
              session.spark.sql(queryText.getQuery4(tmpInput, cuisineType)).show(resultNum, false)
            }
            if (userInput == "5") { //For the restaurant (x) what are its inspection grade, violation code, and violation description?
              df.createOrReplaceTempView("query5")
              var tmpInput: String = ""
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
            if (userInput == "updateAccount") {
              println(s"${YELLOW}Would you like to update your (username) or (password)?${RESET}")
              val userPass = readLine()
              println(s"${YELLOW}Please enter your password to verify your identity.${RESET}")
              val verPass = readLine()
              if (userPass == "username") {
                println(s"${YELLOW}Please enter your new username!${RESET}")
                val newUser = readLine()
                connect.updateAccount(user, verPass, newUser, verPass)
                println(s"${GREEN}Successfully updated!${RESET}")
              }
              else if (userPass == "password") {
                println(s"${YELLOW}Please enter your new password!${RESET}")
                val newPass = readLine()
                connect.updateAccount(user, verPass, user, newPass)
                println(s"${GREEN}Successfully updated!${RESET}")
              } else {
                println(s"${RED}Invalid choice made, returning to menu!${RESET}")
              }
            }
            if (userInput == "addUser" && admin) {
              println(s"${YELLOW}Enter the user's username!${RESET}")
              val tmpUsername = readLine()
              println(s"${YELLOW}Enter the user's password!${RESET}")
              val tmpPassword = readLine()
              connect.addUser(tmpUsername, tmpPassword)
            }
            if (userInput == "deleteUser" && admin) {
              println(s"${YELLOW}Enter the user's username!${RESET}")
              val tmpUsername = readLine()
              println(s"${YELLOW}Attempting to delete...${RESET}")
              if (connect.admin(tmpUsername)) {
                println(s"${RED}Cannot delete admin!${RESET}")
              }
              else if (!connect.exist(tmpUsername)) {
                println(s"${RED}User does not exist!${RESET}")
              }
              else {
                connect.deleteUser(s"${tmpUsername}")
                println(s"${GREEN}Successfully deleted!${RESET}")
              }
            }
            if (userInput == "addRestaurant" && admin) {
              println(s"${YELLOW}Enter the restaurant name!${RESET}")
              val tmpName = readLine()
              println(s"${YELLOW}Enter the restaurant cuisine!${RESET}")
              val tmpCuisine = readLine()
              df = df.unionByName(session.createRestaurant(tmpName.toUpperCase, tmpCuisine), true)
              println(s"${GREEN}Successfully added!${RESET}")
            }
            if (userInput == "deleteRestaurant" && admin) {
              println(s"${YELLOW}Enter the restaurant name!${RESET}")
              val tmpName = readLine().toUpperCase
              df = df.filter(!(col("dba") === tmpName))
              println(s"${GREEN}Successfully deleted!${RESET}")
            }
          } while (userInput != "logout")
        } else {
          println(s"${RED}Your username or password is incorrect!${RESET}")
        }
      }
      if(choice == "2") {
        println(s"${YELLOW}Enter the user's username!${RESET}")
        val tmpUsername = readLine()
        println(s"${YELLOW}Enter the user's password!${RESET}")
        val tmpPassword = readLine()
        connect.addUser(tmpUsername, tmpPassword)
      }
      println(s"Returning to home screen!")
    }
  }
}