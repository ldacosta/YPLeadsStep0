package ypleads

import org.joda.time.DateTime
import util.{ Util => Util }
import common.{ Common => Common }
import common.DataNomenclature._

import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.apache.spark.rdd.RDD
import Common.functions._
import Util._

import scala.util.control.Exception._

object DataExplorer extends Serializable {

  /*
  object RAMSnapshot extends Enumeration {
    type RAMSnapshot = Value
    val COUPLE_OF_HOURS, DAY = Value
  }
  import RAMSnapshot._
  */

}

class DataExplorer(sc: SparkContext) extends Serializable {
  val sqlContext = new org.apache.spark.sql.SQLContext(sc)
  import sqlContext._

  // on init:
  registerParquetTables((new DateTime).withYear(2013).withMonthOfYear(1).withDayOfMonth(1), (new DateTime).withYear(2013).withMonthOfYear(12).withDayOfMonth(31))

  /**
   *
   */
  private def registerParquetTables(fromDate: DateTime, toDate: DateTime) = {
    // accounts
    SparkSQL.parquetTable2TempTable(sqlContext, Accounts.ACCOUNTSPARQUETTABLE, Accounts.TMPACCOUNTSTABLE)
    //
    List(false, true).foreach { online =>
      Date.getAllDays(fromDate, toDate).toList.foreach{ dateToProcess =>
        val theYear = dateToProcess.getYear
        val theMonth = dateToProcess.getMonthOfYear
        val dayOfMonth = dateToProcess.getDayOfMonth
        List(true, false).foreach { createJustASnapshot =>
          try {
            val theDate = (new DateTime).withYear(theYear).withMonthOfYear(theMonth).withDayOfMonth(dayOfMonth)
            RAM.getParquetFullHDFSFileName(theDate, fullDay = !createJustASnapshot, online).map { currentRAMParquetFile =>
              RAM.getHDFSTableName(theDate, fullDay = !createJustASnapshot, online).map { tmpRAMTable =>
                println("Converting from [%s] to [%s]".format(currentRAMParquetFile, tmpRAMTable))
                SparkSQL.parquetTable2TempTable(sqlContext, currentRAMParquetFile, tmpRAMTable)
              }
            }
          }
          catch {
            case e: Exception =>
              println(e.getMessage)
              println("NOT processing %s of day %d".format({if (createJustASnapshot) "full dump" else "sample dump"}, dayOfMonth))
          }
        }
      }
    }
  }


  private def getAllRAMTableNames(fromDate: DateTime, toDate: DateTime, ramSnapshot: Int, online: Boolean): List[String] = {
    Date.getAllDays(fromDate, toDate).toList.foldLeft(List[String]()) { (aList, theDate) =>
      RAM.getHDFSTableName(theDate, fullDay = (ramSnapshot == 1), online).map(List(_)).getOrElse(List[String]()) ++ aList
    }
  }

  private def getQueriesForHeadingMarkets(accountsTableName: String, fromDate: DateTime, toDate: DateTime, ramSnapshot: Int, online: Boolean): List[String] = {
    getAllRAMTableNames(fromDate, toDate, ramSnapshot, online).map{ tmpRAMTable =>
      "SELECT %s.headingId, %s.directoryId, %s.keywords, %s.accountName FROM    %s              JOIN %s         ON %s.accountKey = %s.accountKey".
        format(tmpRAMTable, tmpRAMTable,    tmpRAMTable, accountsTableName,   accountsTableName, tmpRAMTable,     tmpRAMTable,     accountsTableName)
    }
  }

  private def getQueriesForMerchantsAggr(accountsTableName: String, fromDate: DateTime, toDate: DateTime, ramSnapshot: Int, online: Boolean): List[String] = {
    getAllRAMTableNames(fromDate, toDate, ramSnapshot, online).map{ tmpRAMTable =>
      // NB: for some reason when I try to do GROUP BY accountKey, date IT BLOCKS. So I have to do it by code
      s"SELECT ${accountsTableName}.accountKey, ${accountsTableName}.accountName, ${accountsTableName}.accountId,  ${tmpRAMTable}.date,  ${tmpRAMTable}.keywords " +
        s"FROM ${accountsTableName} JOIN ${tmpRAMTable} ON ${accountsTableName}.accountKey = ${tmpRAMTable}.accountKey " // +
        // s"WHERE concat('',${tmpRAMTable}.clicks * 1) = ${tmpRAMTable}.clicks AND ${tmpRAMTable}.clicks > 0" // take only clicks
      // check: http://stackoverflow.com/questions/5064977/detect-if-value-is-number-in-mysql
    }
  }

  private def getDataInAnRDD(queriesToProcess: List[String]): RDD[org.apache.spark.sql.Row] = {
    val allRDDs =
      queriesToProcess.foldLeft(scala.List[RDD[org.apache.spark.sql.Row]]()) { (aList, aQuery) =>
        try {
          println(aQuery)
          sql(aQuery) :: aList
        }
        catch {
          case e: Exception =>
            println(e.getMessage)
            println("NOT processing query %s".format(aQuery))
            aList
        }
      }
    allRDDs.tail.foldLeft(allRDDs.head) { (aggregatedRDD, currentRDD) => aggregatedRDD ++ currentRDD}
  }

  /**
   * Aggregates the results by headings/directory.
   *
   * Assumes that the proper tables (RAM and Accounts) have been properly created with this SQL context.
   *
   */
  def runAggregationPerHeadingAndDirectory(fromDate: DateTime, toDate: DateTime, ramSnapshot: Int, online: Boolean) = {

    case class headingAndDirectory(heading: Long, directory: Long)
    case class queryAndBusinessName(query: String, name: String)
    case class aggregatedResult(heading: Long, directory: Long, queriesTotal: Long, queriesByName: Long, queriesEmpty: Long, queriesNONE: Long) {
      override def toString = {
        "%d\t%d\t%d\t%d\t%d\t%d".format(heading, directory, queriesTotal, queriesByName, queriesEmpty, queriesNONE)
      }
    }

    val outputDir = "results_ypaStep0/headingMarket/%s".format(nowAsString())
    println("RESULTS FROM THIS RUN WILL BE STORED IN hdfs'[%s]".format(outputDir))

    val allData = getDataInAnRDD(getQueriesForHeadingMarkets(Accounts.TMPACCOUNTSTABLE, fromDate, toDate, ramSnapshot, online))
    // NB: fields returned are as defined in <getQueriesForHeadingMarkets>
    // last time I checked it was: headingId, directoryId, keywords, accountName
    // (1) let's group by heading/directory
    val allDataPerAccount = allData.groupBy(aRowOfData => (aRowOfData.getLong(0), aRowOfData.getLong(1)))
    // (2) let's set up proper structures to work with this stuff:
    val headDirQueriesAndNames = allDataPerAccount.map {
      case (headAndDir, rowsWithData) =>
        (headingAndDirectory(heading = headAndDir._1, directory = headAndDir._2),
          rowsWithData.map{r => queryAndBusinessName(query = cleanString(r.getString(2)), name = cleanString(r.getString(3)))}.toList)
    }
    // (3) group and count, man!
    val writableResults = headDirQueriesAndNames.map { case (headAndDir, listOfQueriesAndNames) =>
      val (queriesByName, queriesEMPTY, queriesNONE) =
        listOfQueriesAndNames.foldLeft((0:Long, 0:Long, 0:Long)) { case ((byName, empty, none), queryAndName) =>
          // implicit boolean -> int conversion defined in Util
          (byName + isCloseEnough(queryAndName.query, queryAndName.name),
            empty + queryAndName.query.trim.isEmpty,
            none + (queryAndName.query.trim.toUpperCase == "NONE"))
        }
      aggregatedResult(heading = headAndDir.heading, directory = headAndDir.directory, queriesTotal = listOfQueriesAndNames.length, queriesByName = queriesByName, queriesEmpty = queriesEMPTY, queriesNONE = queriesNONE)
    }
    writableResults.saveAsTextFile(outputDir)
    println("Output written in %s".format(outputDir))
  }


  def runAggregation(fromDate: DateTime, toDate: DateTime, ramSnapshot: Int, online: Boolean) = {
    case class infoToReport(accountKey: Long, accountName: String, accountId: String, queriesTotal: Long, queriesByName: Long, queriesEmpty: Long, queriesNONE: Long) {
      override def toString = {
        "%d\t%s\t%s\t%d\t%d\t%d\t%d".format(accountKey, accountName, accountId, queriesTotal, queriesByName, queriesEmpty, queriesNONE)
      }
    }

    val outputDir = "results_ypaStep0/merchant/%s".format(nowAsString())
    println("RESULTS FROM THIS RUN WILL BE STORED IN hdfs'[%s]".format(outputDir))

    val allData = getDataInAnRDD(getQueriesForMerchantsAggr(Accounts.TMPACCOUNTSTABLE, fromDate, toDate, ramSnapshot, online))
    // NB: fields returned are as defined in <getQueriesForMerchantsAggr>
    // First I group by accountKey + date ==> decomposition of event is aggregated
    case class detailedInfo(accountKey: Long, accountName: String, accountId: String, queries: List[String])
    val keysNamesAndHowManyKeywords: RDD[infoToReport] = allData.
      groupBy(aRow => (aRow.getLong(0), cleanString(aRow.getString(1)), cleanString(aRow.getString(2)), cleanString(aRow.getString(3)))).
      // at this point we have a list of data that looks like this:
      // (((key1, name1, id1, DATE1), List(QUERY1, QUERY1, QUERY1)), ((key1, name1, id1, DATE2), List(QUERY2, QUERY2, QUERY2)), ((key2, name2, id2, DATE3), List(QUERY3, QUERY3, QUERY3)), ...)
      // in other words: for a given date, the query is *always* the same
      // Since I want to group by account, I'd like to have (from the example above):
      // (((key1, name1, id1), List(QUERY1, QUERY2)), ((key2, name2, id2), List(QUERY3)), ...)
      // That is what I accomplish here:
      // (1) get this into a decent structure and discard repeted queries:
      map {case ((accountKey,accountName,accountId,theDate), allQueries) =>
            detailedInfo(accountKey, accountName, accountId, queries = List(allQueries.head(4).toString))
      }.
      // (2) group by accounts, aggregating all queries:
      groupBy{ detInfo => (detInfo.accountKey, detInfo.accountName, detInfo.accountId)}.
      map{ case ((key, name, id), detInfoIterable) => detailedInfo(key, name, id, queries = detInfoIterable.foldLeft(List[String]())((r, dInfo) => r ++ dInfo.queries)) }.
      // (3) finally, count!
      map{d =>
      infoToReport(
        accountKey = d.accountKey, accountName = d.accountName, accountId = d.accountId,
        queriesTotal = d.queries.length,
        queriesByName = d.queries.filter(aQuery => isCloseEnough(aQuery, d.accountName)).length,
        queriesEmpty = d.queries.filter(aQuery => (aQuery.trim == "")).length,
        queriesNONE = d.queries.filter(aQuery => (aQuery.trim.toUpperCase == "NONE")).length)
    }
    /*
    val keysNamesAndHowManyKeywords: RDD[infoToReport] = allData.
      groupBy(aRow => (aRow.getLong(0), cleanString(aRow.getString(1)), cleanString(aRow.getString(2)), cleanString(aRow.getString(3)))).
      map{case ((accountKey,accountName,accountId,theDate), allQueries) => detailedInfo(accountKey, accountName, accountId, queries = allQueries.map(aQuery => aQuery(4).toString).toList)}.
      map{d =>
        infoToReport(
          accountKey = d.accountKey, accountName = d.accountName, accountId = d.accountId,
          queriesTotal = d.queries.length,
          queriesByName = d.queries.filter(aQuery => isCloseEnough(aQuery, d.accountName)).length,
          queriesEmpty = d.queries.filter(aQuery => (aQuery.trim == "")).length,
          queriesNONE = d.queries.filter(aQuery => (aQuery.trim.toUpperCase == "NONE")).length)
      }
      */
    keysNamesAndHowManyKeywords.saveAsTextFile(outputDir)
    println("Output written in %s".format(outputDir))
  }

  /**
   *
   */
  case class RAMEntryRaw(account_key: Long, directory_code: Long, impressionsAsString: String, clicksAsString: String)
  case class AggregationResult(account_id: String, account_key: Long, directory_code: Long, impressions: Long, clicks: Long) {
    override def toString = {
      s"${account_id}\t${account_key}\t${directory_code}\t${impressions}\t${clicks})"
    }
  }
  def adHocSouthShoreRun() = {
    def numAsStrings2Long(theList: Iterable[String]): Long = {
      math.round(theList.foldLeft(0.0) { (doubleSum, aValueAsString) => doubleSum + (catching(classOf[Exception]) opt aValueAsString.toDouble).getOrElse(0.0)})
    }

    val outputDir = "results_adhocSouthShore/%s".format(nowAsString())
    println("RESULTS FROM THIS RUN WILL BE STORED IN hdfs'[%s]".format(outputDir))

    // Create an RDD of objects and register it as a table.
    val rawEntries: RDD[RAMEntryRaw] =
      sc.textFile("/source/ram/export_ram_2012-11.csv")
        .map(_.split(";"))
        .map(p => RAMEntryRaw(account_key = cleanString(p(0).trim).toLong, directory_code = cleanString(p(5).trim).toLong, impressionsAsString = cleanString(p(12).trim), clicksAsString = cleanString(p(13).trim)))
    val southShoreRAM = "caa_ram_southshore_nov2012"
    rawEntries.registerAsTable(s"${southShoreRAM}")
    //
    val theQuery =
      s"SELECT ${Accounts.TMPACCOUNTSTABLE}.accountId, ${southShoreRAM}.account_key,  ${southShoreRAM}.directory_code, ${southShoreRAM}.impressionsAsString, ${southShoreRAM}.clicksAsString " +
        s"FROM ${Accounts.TMPACCOUNTSTABLE} JOIN ${southShoreRAM} " +
        s"ON ${Accounts.TMPACCOUNTSTABLE}.accountKey = ${southShoreRAM}.account_key"

    val rawRDD = sql(theQuery)
    val allResults = rawRDD.groupBy{x => (x.getString(0), x.getLong(1), x.getLong(2))}.
      map{ case ((accountId, accountKey, dirCode), allRows) => (accountId, accountKey, dirCode, allRows.map(_.getString(4)), allRows.map(_.getString(5)))}.
      map { case (accountId, accountKey, dirCode, impressionsAsString, clicksAsString) =>
        AggregationResult(
          account_id = accountId,
          account_key = accountKey,
          directory_code = dirCode,
          impressions = numAsStrings2Long(impressionsAsString),
          clicks = numAsStrings2Long(clicksAsString))
      }
    allResults.saveAsTextFile(outputDir)
    println(s"[adHocSouthShoreRun]Output written in ${outputDir}")
  }

} // class


/*
val qe = new DataExplorer(sc)
qe.runAggregationPerHeadingAndDirectory(fromDay = 10, toDay = 12, ramSnapshot = 0)
qe.runAggregation(10, 12, 0)
 */
