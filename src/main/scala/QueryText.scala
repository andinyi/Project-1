class QueryText {
  def getQuery1():String = "SELECT dba AS Name, cuisine_description AS Cuisine, street as Street, grade AS Grade FROM query1 WHERE Grade = 'B' OR Grade = 'A'"
  def getQuery2():String = "SELECT dba AS Name, cuisine_description AS Cuisine, street as Street, grade AS Grade, critical_flag AS Critical, violation_description AS Description, street AS Street FROM query2 WHERE critical_flag = 'Critical'"
  def getQuery3():String = """SELECT i.dba AS Name, i.cuisine_description AS Cuisine, i.street as Street, i.grade AS Grade, s.isroadwaycompliant as RoadwayCompliant FROM query3i AS i JOIN query3s AS s ON UPPER(i.dba) = UPPER(s.restaurantname) WHERE i.grade = 'C' OR i.grade = 'B' OR i.grade = 'A' UNION SELECT i.dba AS Name, i.cuisine_description AS Cuisine, i.street as Street, i.grade AS Grade, s.isroadwaycompliant as RoadwayCompliant FROM query3i AS i JOIN query3s AS s ON i.bin = s.bin WHERE i.grade = 'C' OR i.grade = 'B' OR i.grade = 'A'"""

  def getQuery4(boro:String, cuisine:String):String =
    {
      s"""SELECT dba AS Name, cuisine_description AS Cuisine, street as Street, grade AS Grade FROM query4 WHERE boro = "${boro}" AND UPPER(cuisine_description) LIKE UPPER("%${cuisine}%")"""
    }

  def getQuery5(name:String):String =
    {
      s"""SELECT dba AS Name, cuisine_description as Cuisine, street as Street, grade as Grade, action as Action, violation_code as ViolationCode, violation_description AS Description FROM query5 WHERE dba = "${name}""""
    }

  def getQuery6():String =
    {
      """SELECT zipcode as Zipcode, AVG(CASE grade WHEN 'A' THEN 4 WHEN 'B' THEN 3 WHEN 'C' THEN 2 WHEN 'D' THEN 1 END) as Grade FROM query6 WHERE grade IS NOT NULL AND zipcode IS NOT NULL GROUP BY zipcode ORDER BY zipcode"""
    }
}
