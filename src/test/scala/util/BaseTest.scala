package util

import org.scalatest.FlatSpec
import util.Util.SQL.PROD_DB_CONNECTION_PARAMS
import util.Util._

class BaseTest extends FlatSpec {

  "A SQL connection" should "be obtained from a valid Server/DB" in {
    val validSQLConnOpt = PROD_DB_CONNECTION_PARAMS.getConnectionOpt
    withClue(s"Connection URL: ==> ${PROD_DB_CONNECTION_PARAMS.connectionURL} <==") { assert(validSQLConnOpt.isDefined) }
    try {
      info(" =======================================> HELLO <=======================================")
      using (validSQLConnOpt.get.createStatement()) { stmt =>
        using (stmt.executeQuery("SELECT * FROM account LIMIT 10")) { rs =>
          while (rs.next()) {
            rs.getString("account_id")
          }
        }
      }
    }
    catch {
      case e: Exception => withClue(s"Connection URL: ${PROD_DB_CONNECTION_PARAMS.connectionURL}; msg = ${e.getMessage}") { assert(false) }
    }
    finally {
      validSQLConnOpt.get.close()
    }
  }

  it should "NOT be obtained if parameters are moronic" in {
    val inValidSQLConnOpt = SQL.getConnectionOpt(server = "lalala", portNumber = 8080, dbName = "", username = "", passwd = "")
    assert(!inValidSQLConnOpt.isDefined)
  }

  "A ResultSet from a valid DB connection" should "be non-empty and iterable" in {
    val validSQLConnOpt = PROD_DB_CONNECTION_PARAMS.getConnectionOpt
    withClue(s"Connection URL: ${PROD_DB_CONNECTION_PARAMS.connectionURL}") { assert(validSQLConnOpt.isDefined) }
    try {
      using (validSQLConnOpt.get.createStatement()) { stmt =>
        using (stmt.executeQuery("SELECT * FROM account LIMIT 10")) { rs =>
          while (rs.next()) {
            rs.getString("account_id")
          }
        }
      }
    }
    catch {
      case e: Exception => withClue(s"Connection URL: ${PROD_DB_CONNECTION_PARAMS.connectionURL}; msg = ${e.getMessage}") { assert(false) }
    }
    finally {
      validSQLConnOpt.get.close()
    }
  }

  it should "be convertible into a Stream" in {
    val validSQLConnOpt = PROD_DB_CONNECTION_PARAMS.getConnectionOpt
    withClue(s"Connection URL: ${PROD_DB_CONNECTION_PARAMS.connectionURL}") { assert(validSQLConnOpt.isDefined) }
    try {
      using (validSQLConnOpt.get.createStatement()) { stmt =>
        using (stmt.executeQuery("SELECT * FROM account LIMIT 10")) { rs =>
          val s = SQL.rs2Stream(rs)
          val streamOfStrings = s.map(rs => rs.getString("account_id"))
          assert(streamOfStrings.size == 10)
          val l = streamOfStrings.foldLeft(List[String]()) { (l, s) => s :: l }
          assert(l.size == streamOfStrings.size)
        }
      }
    }
    catch {
      case e: Exception => withClue(s"Connection URL: ${PROD_DB_CONNECTION_PARAMS.connectionURL}; msg = ${e.getMessage}") { assert(false) }
    }
    finally {
      validSQLConnOpt.get.close()
    }
  }

  it should "be convertible into a List and further usable" in {
    val validSQLConnOpt = PROD_DB_CONNECTION_PARAMS.getConnectionOpt
    withClue(s"Connection URL: ${PROD_DB_CONNECTION_PARAMS.connectionURL}") { assert(validSQLConnOpt.isDefined) }
    try {
      val l1 =
        using (validSQLConnOpt.get.createStatement()) { stmt =>
          using (stmt.executeQuery("SELECT * FROM account LIMIT 10")) { rs =>
            SQL.rs2Stream(rs).toList
          }
        }
      val streamOfStrings = l1.map(rs => rs.getString("account_id"))
      assert(streamOfStrings.size == 10)
      val l = streamOfStrings.foldLeft(List[String]()) { (l, s) => s :: l }
      assert(l.size == streamOfStrings.size)
    }
    catch {
      case e: Exception => withClue(s"Connection URL: ${PROD_DB_CONNECTION_PARAMS.connectionURL}; msg = ${e.getMessage}") { assert(false) }
    }
    finally {
      validSQLConnOpt.get.close()
    }
  }

  "Levenshtein distance on two identical strings" should "be 0" in {
    val s = "luis"
    assert(Levenshtein.distance(s, s) == 0)
  }

  it should "still be zero with 2 other strings" in {
    val s = "luis"
    assert(Levenshtein.distance(s, s) == 0)
  }

  "Transformation of int to string" should "work with months and days" in {
    (1 to 31).foreach { dd => assert(getIntAsString(dd, 2) == {
      if (dd < 10) ("0" + dd) else dd.toString
    })
    }
  }

  /**
   * Things are done like this and NOT with en enumeration of some kind
   *       because of https://issues.apache.org/jira/browse/SPARK-2330 and https://issues.apache.org/jira/browse/SPARK-1199
   *       which block the correct functioning of enumerations in the Spark Shell.
   */
  "Date components" should "fail for unknown components" in {
    assert(verifyDateComponent(12, what = "random struff").isDefined)
  }

  it should "always work for YEAR components" in {
    (1 to 20).foreach { _ =>
      assert(!verifyDateComponent(scala.util.Random.nextInt, what = "YY").isDefined)
    }
  }

  it should "*only* work for decent MONTH components" in {
    (1 to 12).foreach { aMonth =>
      assert(!verifyDateComponent(aMonth, what = "MM").isDefined)
    }
    (-10 to 0).foreach { aMonth =>
      assert(verifyDateComponent(aMonth, what = "MM").isDefined)
    }
    (13 to 33).foreach { aMonth =>
      assert(verifyDateComponent(aMonth, what = "MM").isDefined)
    }
  }

  it should "*only* work for decent DAY components" in {
    (1 to 31).foreach { aMonth =>
      assert(!verifyDateComponent(aMonth, what = "DD").isDefined)
    }
    (-10 to 0).foreach { aMonth =>
      assert(verifyDateComponent(aMonth, what = "DD").isDefined)
    }
    (32 to 45).foreach { aMonth =>
      assert(verifyDateComponent(aMonth, what = "DD").isDefined)
    }
  }

  it should "*only* work for decent HOUR components" in {
    (1 to 24).foreach { aMonth =>
      assert(!verifyDateComponent(aMonth, what = "hh").isDefined)
    }
    (-10 to 0).foreach { aMonth =>
      assert(verifyDateComponent(aMonth, what = "hh").isDefined)
    }
    (25 to 45).foreach { aMonth =>
      assert(verifyDateComponent(aMonth, what = "hh").isDefined)
    }
  }

  it should "*only* work for decent MINUTES components" in {
    (1 to 60).foreach { aMonth =>
      assert(!verifyDateComponent(aMonth, what = "mm").isDefined)
    }
    (-10 to 0).foreach { aMonth =>
      assert(verifyDateComponent(aMonth, what = "mm").isDefined)
    }
    (61 to 90).foreach { aMonth =>
      assert(verifyDateComponent(aMonth, what = "mm").isDefined)
    }
  }

  it should "*only* work for decent SECONDS components" in {
    (1 to 60).foreach { aMonth =>
      assert(!verifyDateComponent(aMonth, what = "ss").isDefined)
    }
    (-10 to 0).foreach { aMonth =>
      assert(verifyDateComponent(aMonth, what = "ss").isDefined)
    }
    (61 to 90).foreach { aMonth =>
      assert(verifyDateComponent(aMonth, what = "ss").isDefined)
    }
  }

}

