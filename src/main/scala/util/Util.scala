package util

import java.io.{PrintWriter, FileWriter}
import java.sql.{DriverManager, Connection, ResultSet}

import org.apache.spark.sql.SQLContext

/**
 * Several Utilities.
 */
object Util extends Serializable {
  import scala.language.implicitConversions

  implicit def bool2int(b:Boolean) = if (b) 1 else 0

  object SQL {

    object PROD_DB_CONNECTION_PARAMS {
      val SERVER_NAME = "ibdevro.itops.ad.ypg.com"
      val SERVER_PORT = 5029
      val DB_NAME = "ypa_pp"
      val DB_USERNAME = "ldcosta1"
      val DB_PASSWD = "ldcosta1"
      val connectionURL = getConnectionURL(server = SERVER_NAME, portNumber = SERVER_PORT, dbName = DB_NAME)
      def getConnectionOpt = SQL.getConnectionOpt(server = SERVER_NAME, portNumber = SERVER_PORT, dbName = DB_NAME, username = DB_USERNAME, passwd = DB_PASSWD)
    }

    def rs2Stream(rs: ResultSet): Stream[ResultSet] =
    {
      new Iterator[ResultSet] { def hasNext = rs.next ;  def next = rs }.toStream
    }

    def getConnectionURL(server: String, portNumber: Int, dbName: String) = s"jdbc:mysql://${server}:${portNumber}/${dbName}"

    def getConnectionOpt(server: String, portNumber: Int, dbName: String, username: String, passwd: String): Option[Connection] = {
      val jdbcURL = getConnectionURL(server = server, portNumber = portNumber, dbName = dbName)
      try {
        Some(DriverManager.getConnection(jdbcURL, username, passwd))
      }
      catch {
        case e: Exception =>
          println(s"Failure connecting to ${jdbcURL}, user = ${username}, passwd = <provided by the user>. Do 'printStackTrace' if you need more details.")
          // e.printStackTrace()
          None
      }
    }

  }

  /**
   *
   * @param value
   * @param what
   * @note Things are done like this and NOT with en enumeration of some kind
   *       because of https://issues.apache.org/jira/browse/SPARK-2330 and https://issues.apache.org/jira/browse/SPARK-1199
   *       which block the correct functioning of enumerations in the Spark Shell.
   * @return a String with an error message, in case of failure. None otherwise.
   */
  def verifyDateComponent(value: Int, what: String): Option[String] = {
    what.trim.substring(0, 2) match {
      case "YY" => None // year can never be wrong, basically
      case "MM" =>
        if ((value < 1) || (value > 12)) Some(s"Month must be in [1,12]: currently ${value}")
        else None
      case "DD" =>
        if ((value < 1) || (value > 31)) Some(s"Day must be in [1,31]: currently ${value}")
        else None
      case "hh" =>
        if ((value < 1) || (value > 24)) Some(s"Hour of day must be in [1,24]: currently ${value}")
        else None
      case "mm" =>
        if ((value < 1) || (value > 60)) Some(s"Minutes of the hour must be in [1,60]: currently ${value}")
        else None
      case "ss" =>
        if ((value < 1) || (value > 60)) Some(s"Seconds of the minute must be in [1,12]: currently ${value}")
        else None
      case _ => Some(s"I don't understand what componet of a date ${what} is")
    }
  }

  /**
   *
   * @param dayOrMonth
   * @return
   */
  def getIntAsString(dayOrMonth: Int, minStringLength: Int): String = {
    val s = dayOrMonth.toString
    (1 to (minStringLength - s.length)).toList.foldLeft(""){ (r, _) => "0" + r } + s
  }

  /**
   * Returns current date as a String.
   * @param dateFormat
   * @return
   */
  def nowAsString() =
  {
    val dateFormat: java.text.SimpleDateFormat = new java.text.SimpleDateFormat("yy.MM.dd_hh.mm.ss")
    val now = java.util.Calendar.getInstance().getTime()
    dateFormat.format(now)
  }

  /**
   * Used for reading/writing to database, files, etc.
   * Code From the book "Beginning Scala"
   * http://www.amazon.com/Beginning-Scala-David-Pollak/dp/1430219890
   */
  def using[A <: {def close(): Unit}, B](param: A)(f: A => B): B =
    try { f(param) } finally { param.close() }

  def writeToFile(fileName:String, data:String) =
    using (new FileWriter(fileName)) {
      fileWriter => fileWriter.write(data)
    }

  def appendToFile(fileName:String, textData:String) =
    using (new FileWriter(fileName, true)){
      fileWriter => using (new PrintWriter(fileWriter)) {
        printWriter => printWriter.println(textData)
      }
    }


  import scala.math._

  object Levenshtein extends Serializable {
    def minimum(i1: Int, i2: Int, i3: Int)=min(min(i1, i2), i3)
    def distance(s1:String, s2:String)={
      val dist=Array.tabulate(s2.length+1, s1.length+1){(j,i)=>if(j==0) i else if (i==0) j else 0}

      for(j<-1 to s2.length; i<-1 to s1.length)
        dist(j)(i)= {
          if(s2.charAt(j-1) == s1.charAt(i-1)) dist(j-1)(i-1)
          else minimum(dist(j-1)(i)+1, dist(j)(i-1)+1, dist(j-1)(i-1)+1)
        }

      dist(s2.length)(s1.length)
    }

    def main(args: Array[String]): Unit = {
      printDistance("kitten", "sitting")
      printDistance("rosettacode", "raisethysword")
    }

    def printDistance(s1:String, s2:String)=println("%s -> %s : %d".format(s1, s2, distance(s1, s2)))
  }

  object SparkSQL extends Serializable {
    def parquetTable2TempTable(sqlContext: SQLContext, nameParquetTable: String, nameTable: String) = {
      val accountsAsParquet = sqlContext.parquetFile(nameParquetTable)
      accountsAsParquet.registerAsTable(nameTable)
    }
  }


}
