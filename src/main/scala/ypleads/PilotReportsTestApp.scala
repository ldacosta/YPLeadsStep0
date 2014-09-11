package ypleads

/**
 * Sanity tests for reports for YP Leads Pilot reports.
 */

import java.sql.{SQLException, Connection}
import org.joda.time.DateTime
import util.{ Util => LocalUtils }
import LocalUtils.SQL._
import LocalUtils._
import scala.util.control.Exception._

object PilotReportsTestApp {

  /* ********************************************************* */
  // TODO: move this to a saner location
  trait Wrapper[T]

  trait StringWrapper extends Wrapper[String] {
    val value: String
  }

  trait DateString extends StringWrapper {
    def toDateTime: DateTime = string2Date(value).get
  }
  object DateString {
    def apply(s:String): Option[DateString] = {
      if (checkDate(s)) Some(DateStringImpl(s))
      else None
    }

    private case class DateStringImpl(value: String) extends DateString
  }


  private val datePattern = """(\d{4})-(\d{2})-(\d{2})""".r
  // I am expecting the date to come in format YYYY-MM-DD, as specified here: https://wiki.ypg.com/display/CAA/CAA+YP+Leads+Pilot+Report+Generation+and+Testing

  private[ypleads] def string2Date(aDateAsString: String): Option[DateTime] = {
    try {
      val datePattern(yAsString, mAsString, dAsString) = aDateAsString
      Some(DateTime.now().withYear(yAsString.toInt).withMonthOfYear(mAsString.toInt).withDayOfMonth(dAsString.toInt))
    }
    catch {
      case e: Exception =>
        println(s"${aDateAsString} is not a proper date.")
        None
    }
  }

  private[ypleads] def checkDate(aDateAsString: String): Boolean = {
    string2Date(aDateAsString).isDefined
  }

  /* ********************************************************* */

  /**
   * Parses parameters from command line and fills up a map with the extracted values.
   */
  private def parseParams(args: Array[String]): Map[String, String] = {
    def parseNextParam(argsList: List[String], m: Map[String, String]): Map[String, String] = {
      argsList match {
        case Nil => m
        case "--passwd" :: passwdValue :: t => parseNextParam(t, m + ("passwd" -> passwdValue))
        case "--accountId" :: accountIdValue :: t => parseNextParam(t, m + ("accountId" -> accountIdValue))
        case "--beginDate" :: beginDateValue :: t => {
          DateString(beginDateValue).map { _ => parseNextParam(t, m + ("beginDate" -> beginDateValue))}.getOrElse(m)
        }
        case "--endDate" :: endDateValue :: t => {
          DateString(endDateValue).map { _ => parseNextParam(t, m + ("endDate" -> endDateValue))}.getOrElse(m)
        }
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

  private def dateTimeToProperStringFormat(theDate: DateTime): String = {
    "%04d".format(theDate.getYear) + "/" + "%02d".format(theDate.getMonthOfYear) + "/" + "%02d".format(theDate.getDayOfMonth) + "-" + "%02d".format(theDate.getHourOfDay) + ":" + "%02d".format(theDate.getMinuteOfHour) + ":" + "%02d".format(theDate.getSecondOfMinute)
  }

  private def allOnlineRecordsQuery(merchantKeys: List[Long], beginDate: DateTime, endDate: DateTime): String = {
    val beginDateAsString = dateTimeToProperStringFormat(beginDate)
    val endDateAsString = dateTimeToProperStringFormat(endDate)
    val q =
    s"""SELECT raw_activity_account_key, account_key, session_id, activity_date
                  |FROM raw_activity_metrics RAM
                  |WHERE account_key IN ${merchantsList2SQLList(merchantKeys)}
                  |AND DATE_FORMAT(activity_date, '%Y/%m/%d-%T') >= '${beginDateAsString}' AND DATE_FORMAT(activity_date, '%Y/%m/%d-%T') <= '${endDateAsString}'
                  |AND (click_type_id IN (SELECT click_type_id FROM click_type WHERE name IN ${leadsClickTypesAsSQLList} ) OR click_type_id = 0)
                  |""".stripMargin
    println(q)
    q
  }

  case class ReportOnlineRecord(date_day: String, merchant_id: String, cust_account_id: String, account_name: String, impressions: Long, click_type_name: String, clicks: Long, platform: String, source: String, keyword: String, heading_name: String, directory_name: String)

  // TODO: check, beginDate should be -00:00:00 and endDate 23:59:59
  def getReportOnlineRecords(conn: Connection, merchantIds: List[Long], beginDate: DateTime, endDate: DateTime): List[ReportOnlineRecord] = {
    val q: String = {
      val beginDateAsString = dateTimeToProperStringFormat(beginDate)
      val endDateAsString = dateTimeToProperStringFormat(endDate)
      s"""SELECT * FROM yp_leads_list
       |WHERE merchant_id IN ${merchantsList2SQLList(merchantIds)}
       |AND DATE_FORMAT(date_day, '%Y/%m/%d-%T') >= '${beginDateAsString}' AND DATE_FORMAT(date_day, '%Y/%m/%d-%T') <= '${endDateAsString}'
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
      println(s"Format of dates is ${datePattern.toString()}")
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
          val reportOnlineData = getReportOnlineRecords(connToReportResults, merchantIds = merchantIDsForAccount, beginDate = DateString(params.get("beginDate").get).get.toDateTime.withHourOfDay(0).withMinuteOfHour(0).withSecondOfMinute(0), endDate = DateString(params.get("endDate").get).get.toDateTime.withHourOfDay(23).withMinuteOfHour(59).withSecondOfMinute(59))
          val reportPhoneData = getReportPhoneRecords(connToReportResults, merchantIds = merchantIDsForAccount, beginDate = params.get("beginDate").get, endDate = params.get("endDate").get)
          val numberOfOnlineRecords =
            using(connToProdDB.createStatement()) { stmt =>
              val q = allOnlineRecordsQuery(merchantKeys = merchantKeysForAccount, beginDate = DateString(params.get("beginDate").get).get.toDateTime.withHourOfDay(0).withMinuteOfHour(0).withSecondOfMinute(0), endDate = DateString(params.get("endDate").get).get.toDateTime.withHourOfDay(23).withMinuteOfHour(59).withSecondOfMinute(59))
              rs2Stream(stmt.executeQuery(q)).size
            }
          val numberOfPhoneRecords =
            using(connToProdDB.createStatement()) { stmt =>
              val q = allPhoneRecordsQuery(merchantKeys = merchantKeysForAccount, beginDate = params.get("beginDate").get, endDate = params.get("endDate").get)
              rs2Stream(stmt.executeQuery(q)).size
            }
          // Test: are the number of records in report what they are supposed to be?
          val onlineRecordsOK = (numberOfOnlineRecords == reportOnlineData.size)
          val phoneRecordsOK = (numberOfPhoneRecords == reportPhoneData.size)
          println(s"Test [are the number of records in *online* report (${reportOnlineData.length}) what they are supposed to be (${numberOfOnlineRecords})]: ${if (onlineRecordsOK) "SUCCESS" else "FAILED"}")
          println(s"Test [are the number of records in *phone* report (${reportPhoneData.size}) what they are supposed to be (${numberOfPhoneRecords})]: ${if (phoneRecordsOK) "SUCCESS" else "FAILED"}")
          // We will only run individual tests in number of records are OK:
          if (onlineRecordsOK && phoneRecordsOK) {
            // Test:
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
              // one, and *only* one, of clicks or impressions are > 0
              if (!((aRecord.clicks * aRecord.impressions == 0L) && (aRecord.clicks + aRecord.impressions > 0L))) {
                println(s"[${idOfRecord}] One, and *only* one, of clicks or impressions have to be > 0; now: clicks = ${aRecord.clicks}, impressions = ${aRecord.impressions}")
              }
            }
            println("******************* END INDIVIDUAL RECORDS TESTS ************************")
          }
          connToProdDB.close()
        }
        connToReportResults.close()
      }
    }
  }
}
