package ypleads

/**
 * Sanity tests for reports for YP Leads Pilot reports.
 */

import java.sql.{SQLException, ResultSet, Connection}
import util.{ Util => LocalUtils }
import LocalUtils.SQL._
import LocalUtils._
import scala.util.control.Exception._

object PilotReportsTestApp {

  /**
   * Parses parameters from command line and fills up a map with the extracted values.
   */
  private def parseParams(args: Array[String]): Map[String, String] = {
    def parseNextParam(argsList: List[String], m: Map[String, String]): Map[String, String] = {
      argsList match {
        case Nil => m
        case "--passwd" :: passwdValue :: t => parseNextParam(t, m + ("passwd" -> passwdValue))
        case "--accountId" :: accountIdValue :: t => parseNextParam(t, m + ("accountId" -> accountIdValue))
          // TODO: verify date
        case "--beginDate" :: beginDateValue :: t => parseNextParam(t, m + ("beginDate" -> beginDateValue))
        // TODO: verify date
        case "--endDate" :: endDateValue :: t => parseNextParam(t, m + ("endDate" -> endDateValue))
        case param :: t => parseNextParam(t, m)
      }
    }
    parseNextParam(args.toList, Map[String, String]())
  }

  // Get merchant IDS, given an account id.
  private def getMerchantsIDSOrKeysForAccount(connectionToProdDB: Connection, accountId: String, fieldName: String): List[Long] = {

    val query = s"SELECT ${fieldName} FROM account_hierarchy_bridge WHERE parent_account_id = '${accountId}'"
    println(s"fieldName = ${fieldName}; query = ${query}")
    using (connectionToProdDB.createStatement()) { stmt =>
      rs2Stream(stmt.executeQuery(query)).flatMap(rs => { catching(classOf[SQLException]) opt { rs.getLong(s"${fieldName}") } }).toList
    }
  }

  private[ypleads] def getMerchantsIDsForAccount(connectionToProdDB: Connection, accountId: String): List[Long] = {
    getMerchantsIDSOrKeysForAccount(connectionToProdDB, accountId, fieldName = "child_account_id")
  }

  private[ypleads] def getMerchantsKeysForAccount(connectionToProdDB: Connection, accountId: String): List[Long] = {
    getMerchantsIDSOrKeysForAccount(connectionToProdDB, accountId, fieldName = "child_account_key")
  }

  private[ypleads] def merchantsList2SQLList(merchantsList: List[Long]) = s"('${merchantsList.mkString("','")}')"

  private def allPhoneRecordsQuery(merchantKeys: List[Long], beginDate: String, endDate: String): String = {
    s"""
       |SELECT * FROM phone_lead PL
       |WHERE account_key IN ${merchantsList2SQLList(merchantKeys)}
       |AND DATE_FORMAT(created_datetime, '%Y/%m/%d-%T') >= '${beginDate}-00:00:00' AND DATE_FORMAT(created_datetime, '%Y/%m/%d-%T') <= '${endDate}-23:59:59'
     """.stripMargin
  }

  // As defined in https://wiki.ypg.com/display/CAA/Weekly+report+on+contacts+metrics+send+to+YP+Leads+Customers
  private val leadsAsClickTypeNames = List("appointment_submit","contact_submit","email-confirm","newsletter_submit","quote_submit",
    "reserve_online","reservenow-ot","directions-confirm","printdirection","free_call","free_call_mp",
    "saveshare_email-confirm","sms-confirm","howtodetail","share_confirm","save_share_twitter",
    "save_share_facebook","save_share_tripadvisor","save_share_foursquare","save_share_other",
    "saveshareemail-confirm","howtodetail","share_confirm")

  private val leadsClickTypesAsSQLList = s"('${leadsAsClickTypeNames.mkString("','")}')"

  private def allOnlineRecordsQuery(merchantKeys: List[Long], beginDate: String, endDate: String): String = {
    s"""SELECT raw_activity_account_key, account_key, session_id, activity_date
                  |FROM raw_activity_metrics RAM
                  |WHERE account_key IN ${merchantsList2SQLList(merchantKeys)}
                  |AND DATE_FORMAT(activity_date, '%Y/%m/%d-%T') >= '${beginDate}-00:00:00' AND DATE_FORMAT(activity_date, '%Y/%m/%d-%T') <= '${endDate}-23:59:59'
                  |AND click_type_id IN (SELECT click_type_id FROM click_type WHERE name IN ${leadsClickTypesAsSQLList} )
                  |""".stripMargin
  }

  case class ReportOnlineRecord(date_day: String, merchant_id: String, cust_account_id: String, account_name: String, impressions: Long, click_type_name: String, clicks: Long, platform: String, source: String, keyword: String, heading_name: String, directory_name: String)

  def getReportOnlineRecords(conn: Connection, merchantIds: List[Long], beginDate: String, endDate: String): List[ReportOnlineRecord] = {
    val q: String = {
      s"""SELECT * FROM yp_leads_list
       |WHERE merchant_id IN ${merchantsList2SQLList(merchantIds)}
       |AND DATE_FORMAT(date_day, '%Y/%m/%d-%T') >= '${beginDate}-00:00:00' AND DATE_FORMAT(date_day, '%Y/%m/%d-%T') <= '${endDate}-23:59:59'
     """.stripMargin
    }
    using (conn.createStatement()) { stmt =>
      rs2Stream(stmt.executeQuery(q)).map{ rs =>
        ReportOnlineRecord(
          date_day = rs.getString("date_day"), merchant_id = rs.getString("merchant_id"), cust_account_id = rs.getString("cust_account_id"),
          account_name = rs.getString("account_name"), impressions = rs.getLong("impressions"), click_type_name = rs.getString("click_type_name"),
          clicks = rs.getLong("clicks"), platform = rs.getString("platform"), source = rs.getString("source"),
          keyword = rs.getString("keyword"), heading_name = rs.getString("heading_name"), directory_name = rs.getString("directory_name"))
      }.toList
    }
  }

  case class ReportPhoneRecord(phone_lead_date: String, phone_lead_time: String, merchant_id: String, cust_account_id: String, account_name: String, Zone: String, duration: Int, target_phone: String, source_phone: String, location_city: String, location_zip_code: String, answer_status: String)

  def getReportPhoneRecords(conn: Connection, merchantIds: List[Long], beginDate: String, endDate: String): List[ReportPhoneRecord] = {
    val q: String = {
      s"""SELECT * FROM phone_yp_leads
       |WHERE merchant_id IN ${merchantsList2SQLList(merchantIds)}
       |AND DATE_FORMAT(phone_lead_date, '%Y/%m/%d-%T') >= '${beginDate}-00:00:00' AND DATE_FORMAT(phone_lead_date, '%Y/%m/%d-%T') <= '${endDate}-23:59:59'
     """.stripMargin
    }
    using(conn.createStatement()) { stmt =>
      rs2Stream(stmt.executeQuery(q)).map { rs =>
        ReportPhoneRecord(
          phone_lead_date = rs.getString("phone_lead_date"), phone_lead_time = rs.getString("phone_lead_time"), merchant_id = rs.getString("merchant_id"),
          cust_account_id = rs.getString("cust_account_id"), account_name = rs.getString("account_name"), Zone = rs.getString("Zone"),
          duration = rs.getInt("duration"), target_phone = rs.getString("target_phone"), source_phone = rs.getString("source_phone"),
          location_city = rs.getString("location_city"), location_zip_code = rs.getString("location_zip_code"), answer_status = rs.getString("answer_status"))
      }.toList
    }
  }


  def main(args: Array[String]) {

    val params: Map[String, String] = parseParams(args)
    if (!params.contains("passwd") || !params.contains("accountId") || !params.contains("beginDate") || !params.contains("endDate")) {
      if (!params.contains("passwd")) println("--passwd NOT specfied")
      if (!params.contains("accountId")) println("--accountId NOT specfied")
      if (!params.contains("beginDate")) println("--beginDate NOT specfied")
      if (!params.contains("endDate")) println("--endDate NOT specfied")
      println("Usage: --passwd <passwd> --accountId <accountId> --beginDate <beginDate> --endDate <endDate>")
      println("Exiting now.")
    }
    else {
      // "copy of production" server =  "172.19.63.9"; option.get will not fail. See the 'if' above.
      getConnectionOpt(server = "ypgldcctsqdb01.itops.ad.ypg.com", portNumber = 3306, dbName = "test", username = "ldacosta1", passwd = params.get("passwd").get).map { connToReportResults =>
        PROD_DB_CONNECTION_PARAMS.getConnectionOpt.map { connToProdDB =>
          println("OK, we are IN!")
          val merchantIDsForAccount = getMerchantsIDsForAccount(connToProdDB, accountId = params.get("accountId").get)
          val merchantKeysForAccount = getMerchantsKeysForAccount(connToProdDB, accountId = params.get("accountId").get)
          println(s"Account id = '${params.get("accountId").get}' have ${merchantIDsForAccount.length} merchants *IDS* associated = {${merchantIDsForAccount.mkString(",")}}")
          println(s"Account id = '${params.get("accountId").get}' have ${merchantIDsForAccount.length} merchants *KEYS* associated = {${merchantKeysForAccount.mkString(",")}}")
          val reportOnlineData = getReportOnlineRecords(connToReportResults, merchantIds = merchantIDsForAccount, beginDate = params.get("beginDate").get, endDate = params.get("endDate").get)
          val reportPhoneData = getReportPhoneRecords(connToReportResults, merchantIds = merchantIDsForAccount, beginDate = params.get("beginDate").get, endDate = params.get("endDate").get)
          val numberOfOnlineRecords =
            using(connToProdDB.createStatement()) { stmt =>
              val q = allOnlineRecordsQuery(merchantKeys = merchantKeysForAccount, beginDate = params.get("beginDate").get, endDate = params.get("endDate").get)
              rs2Stream(stmt.executeQuery(q)).size
            }
          val numberOfPhoneRecords =
            using(connToProdDB.createStatement()) { stmt =>
              val q = allPhoneRecordsQuery(merchantKeys = merchantKeysForAccount, beginDate = params.get("beginDate").get, endDate = params.get("endDate").get)
              rs2Stream(stmt.executeQuery(q)).size
            }
          // Test: are the number of records in report what they are supposed to be?
          println(s"Test [are the number of records in *online* report (${reportOnlineData.length}) what they are supposed to be (${numberOfOnlineRecords})]: ${if (numberOfOnlineRecords == reportOnlineData.size) "SUCCESS" else "FAILED"}")
          println(s"Test [are the number of records in *phone* report (${reportPhoneData.size}) what they are supposed to be (${numberOfPhoneRecords})]: ${if (numberOfPhoneRecords == reportPhoneData.size) "SUCCESS" else "FAILED"}")
          // Test: TODO
          println("******************* BEGIN INDIVIDUAL RECORDS TESTS ************************")
          println("A list of warnings or possible errors will be printed below... if any found")
          reportOnlineData.foreach { aRecord =>
            val idOfRecord = s"date_day = ${aRecord.date_day}, merchant_id = ${aRecord.merchant_id}"
            // TODO: unit test: aRecord("date_day") is in the date asked
            // TODO: unit test: aRecord("merchant_id") is in the list expected
            // TODO: etc. like above.
            // platform OK?
            aRecord.platform.toUpperCase match {
              case p if (p.isEmpty) => println(s"Record ${idOfRecord} has EMPTY platform")
              case p if ((p != "DESKTOP") && (p != "MOBILE")) => println(s"Record ${idOfRecord} has platform = '${aRecord.platform}', but only 'Desktop' and 'Mobile' are accepted")
              case _ =>
            }
            // click types OK?
            if (!leadsAsClickTypeNames.contains(aRecord.click_type_name)) {
              println(s"Record ${idOfRecord} has click_type_name = '${aRecord.click_type_name}', which is *NOT* a lead")
            }
            // clicks OK?
            if (aRecord.clicks == 0L) {
              println(s"Record ${idOfRecord} has clicks == 0")
            }
          }
          println("******************* END INDIVIDUAL RECORDS TESTS ************************")
          connToProdDB.close()
        }
        connToReportResults.close()
      }
    }
  }
}
