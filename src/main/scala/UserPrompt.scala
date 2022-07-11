import Console.{GREEN, RED, RESET, UNDERLINED, YELLOW, CYAN}

class UserPrompt {
  def printStart():Unit =
  {
    println("Welcome to Andy's Restaurant Data Emporium!\n" +
      "The program will now run! Please follow the choices offered by the menu!")
  }

  def printUserMenu():Unit =
    {
      println(s"(1) ${YELLOW}Show all restaurants with a sanitary rating B or above!${RESET}\n" +
              s"(2) ${YELLOW}Show all restaurants with a critical warning/flag.${RESET}\n" +
              s"(3) ${YELLOW}Show all of the restaurants that have an Inspection Grade above B and also provide outdoors seating because of COVID.${RESET}\n" +
              s"(4) ${YELLOW}Show all of the restaurants given a borough to search in, as well as a cuisine type to filter by.${RESET}\n" +
              s"(5) ${YELLOW}Show the details of a specific restaurant, by name.${RESET}\n" +
              s"(6) ${YELLOW}Show the average grade of restaurants by Zipcode (Average Represented Numerically From 1-3).${RESET}\n" +
              s"(setRows) ${YELLOW}Set the global row desired when printing out queries.${RESET}\n" +
              s"(updateAccount) ${YELLOW}Update your account login information.${RESET}\n" +
              s"(logout) ${RED}Logout of the system${RESET}\n" +
              s"(exit) ${RED}Close Program${RESET}")
    }

  def printAdminMenu():Unit =
  {
    println(s"(1) ${YELLOW}Show all restaurants with a sanitary rating B or above!${RESET}\n" +
      s"(2) ${YELLOW}Show all restaurants with a critical warning/flag.${RESET}\n" +
      s"(3) ${YELLOW}Show all of the restaurants that have an Inspection Grade above B and also provide outdoors seating because of COVID.${RESET}\n" +
      s"(4) ${YELLOW}Show all of the restaurants given a borough to search in, as well as a cuisine type to filter by.${RESET}\n" +
      s"(5) ${YELLOW}Show the details of a specific restaurant, by name.${RESET}\n" +
      s"(6) ${YELLOW}Show the average grade of restaurants by Zipcode (Average Represented Numerically From 1-3).${RESET}\n" +
      s"(setRows) ${YELLOW}Set the global row desired when printing out queries.${RESET}\n" +
      s"(updateAccount) ${YELLOW}Update your account login information.${RESET}\n" +
      s"(addUser) ${CYAN}Add an user to the system.${RESET}\n" +
      s"(deleteUser) ${CYAN}Delete an user from the system.${RESET}\n" +
      s"(addRestaurant) ${CYAN}Add an restaurant to the database.${RESET}\n" +
      s"(deleteRestaurant) ${CYAN}Delete an restaurant from the database.${RESET}\n" +
      s"(logout) ${RED}Logout of the system${RESET}\n" +
      s"(exit) ${RED}Close Program${RESET}")
  }

  def printSetResults():Unit =
    {
      println(s"${YELLOW}You have chosen set the amount of rows printed by your queries!${RESET}\n" +
              s"${YELLOW}Please enter your desired number of roles! This will be applied globally!${RESET}")
    }
}
