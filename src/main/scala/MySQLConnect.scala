import java.sql.{Connection,DriverManager}
import Console.{GREEN, RED, RESET, UNDERLINED, YELLOW, CYAN}


class MySQLConnect {
  val url = "jdbc:mysql://localhost:3306/project1auth"
  val driver = "com.mysql.cj.jdbc.Driver"
  val username = "andinyi"
  val password = "MYpassword1SQL!"
  var connection:Connection = _
  try {
    Class.forName(driver)
    connection = DriverManager.getConnection(url, username, password)
  }

  def authMatch(user:String, pass:String):Boolean = {
    val statement = connection.createStatement()
    val check = statement.executeQuery(s"""SELECT user, password FROM users WHERE user = "${user}"""")
    while(check.next())
      {
        val tmpUser = check.getString("user")
        val password = check.getString("password")
        if(tmpUser == user && password == pass)
          {
            return true
          }
      }
    false
  }

  def admin(user:String):Boolean = {
    val statement = connection.createStatement()
    val check = statement.executeQuery(s"""SELECT admin FROM users WHERE user = "${user}"""")
    while(check.next())
      {
        val tmp = check.getBoolean("admin")
        return tmp
      }
      false
  }

  def exist(user:String):Boolean = {
    val statement = connection.createStatement()
    val check = statement.executeQuery(s"""SELECT user FROM users WHERE user = "${user}"""")
    if(check.next()) {
      return true
    }
    false
  }

  def updateAccount(user:String, pass:String, newUser:String, newPass:String):Boolean = {
    if(!authMatch(user, pass)) {
      println(s"${RED}Your username or password does not match. Cannot update your account${RESET}")
      false
    } else {
      val statement = connection.createStatement()
      statement.execute(s"""UPDATE users SET user = "${newUser}", password = "${newPass}" WHERE user = "${user}"""")
      true
    }
  }

  def addUser(user:String, pass:String):Unit = {
    val statement = connection.createStatement()
    try {
      statement.execute(s"""INSERT INTO users (user, password) VALUES ("${user}", "${pass}")""")
    }
  }

  def deleteUser(user:String):Unit = {
    val statement = connection.createStatement()
    try {
      statement.execute(s"""DELETE FROM users WHERE user = "${user}"""")
    }
  }
}
