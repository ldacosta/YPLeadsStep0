/**
 * JDBC implementation of YPLeads version 0 queries.
 */

import java.sql.{DriverManager, Connection}

object JDBCExplorationApp {

  private def parseParams(args: Array[String]): Map[String, String] = {
    def parseNextParam(argsList: List[String], m: Map[String, String]): Map[String, String] = {
      argsList match {
        case Nil => m
        case "--passwd" :: passwdValue :: t => parseNextParam(t, m + ("passwd" -> passwdValue))
        case param :: t => parseNextParam(t, m)
      }
    }
    parseNextParam(args.toList, Map[String, String]())
  }

  def main(args: Array[String]) {

    val params: Map[String, String] = parseParams(args)
    if (!params.contains("passwd")) {
      println("Usage: --passwd: <passwd>")
      println("No password specified. Exiting now.")
    }
    else {
      val server = "ibdevro.itops.ad.ypg.com"
      val portNumber = 5029
      val dbName = "ypa_dev"
      val username = "ldcosta1"
      val passwd = params.get("passwd").get // will not fail. See the 'if' above.
      val jdbcURL = s"jdbc:mysql://${server}:${portNumber}/${dbName}"

      val connOpt: Option[Connection] =
        try {
          Some(DriverManager.getConnection(jdbcURL, username, passwd))
        }
        catch {
          case e: Exception =>
            println(s"Failure connecting to ${jdbcURL}, user = ${username}, passwd = <provided by the user>. Do 'printStackTrace' if you need more details.")
            None
        }
      connOpt.map{ conn =>
        println("OK, we are IN!")
        val stmt = conn.createStatement()
        val rsSet = stmt.executeQuery("SELECT COUNT(*) FROM account")
        if (rsSet.next())
          println(s"Table 'account' has ${rsSet.getInt(1)} rows")
        else
          println("Error fetching row count from table 'account'")
        stmt.close()
        conn.close()
      }
    }
  }

}
