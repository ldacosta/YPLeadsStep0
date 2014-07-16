/**
 * JDBC implementation of YPLeads version 0 queries.
 */

import java.sql.{DriverManager, Connection}

object JDBCExplorationApp {

  /**
   * Parses parameters from command line and fills up a map with the extracted values.
   */
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

  /**
   * Builds a query string that returns all activities from RAM.
   */
  private def mkQueryStringForActivities(filterSearchesByName: Boolean, dateFilterAsString: String): String = {
    "(select keyword,activity_date,account_key from raw_activity_metrics WHERE DATE(activity_date) " + dateFilterAsString + ") ram " +
      "JOIN " +
      "account a " +
      "ON " +
      "a.account_key = ram.account_key JOIN location l " +
      "ON a.account_location_key = l.location_key " +
      {
        if (filterSearchesByName)
          "WHERE ucase(REPLACE(REPLACE(ram.keyword,\"-\",\"\"),\" \",\"\")) <> ucase(replace(replace(a.account_name, \"'\", \"\"),\" \",\"\")) "
        else
          " "
      } +
      "GROUP BY a.account_key " +
      "ORDER BY a.account_name" +
      ")"
  }

  /**
   * We want to filter the RAM table for activity happening following this filter.
   */
  private val DATE_FILTER_AS_STRING = "< \"2013-08-01\"" // "> \"2014-03-01\""

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
      val query = "SELECT " +
        "tAll.account_key, tAll.account_id, tAll.account_name, tAll.location_city, tAll.location_zip_code, tAll.totalCount, tDiff.perCategoryCount, tDiff.perCategoryCount / tAll.totalCount AS categoryProportion FROM " +
        "(select a.account_key, a.account_id, a.account_name, l.location_city, l.location_zip_code, COUNT(*) As totalCount " +
        "FROM " +
        mkQueryStringForActivities(false, DATE_FILTER_AS_STRING) + " tAll " +
        "LEFT JOIN " +
        "(select a.account_key, COUNT(*) As perCategoryCount " +
        "FROM " +
        mkQueryStringForActivities(true, DATE_FILTER_AS_STRING) + " tDiff " +
        "ON tAll.account_key = tDiff.account_key;"

      println(query)
      connOpt.map{ conn =>
        println("OK, we are IN!")
        val stmt = conn.createStatement()
        val rsSet = stmt.executeQuery(query)
        if (rsSet.next())
          println(s"Query successfully executed")
        else
          println(s"Query execution failed")
        stmt.close()
        conn.close()
      }
    }
  }

}
