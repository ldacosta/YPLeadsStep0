package ypleads

import util.{ Util => Util }
import common.{ Base => Base }

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import Util._
import Base.functions._
import Base.constants._


object DataExplorer extends Serializable {

  /*
  object RAMSnapshot extends Enumeration {
    type RAMSnapshot = Value
    val COUPLE_OF_HOURS, DAY = Value
  }
  import RAMSnapshot._
  */

  val TMPRAMTABLEPREFIX = "caa_ram"
  val TMPACCOUNTSTABLE = "caa_accounts"

  def getRAMTableName(dayOfMonth: Int, isSnapshot: Boolean): String = {
    val currentRAMParquetPrefix = {
      if (isSnapshot) "%s_%d_sample".format(mayParquetFilesPrefix, dayOfMonth)
      else "%s_%d".format(mayParquetFilesPrefix, dayOfMonth)
    }
    "%s_%s".format(TMPRAMTABLEPREFIX, currentRAMParquetPrefix)
  }

}

class DataExplorer(sc: SparkContext) extends Serializable {

  import DataExplorer._

  val sqlContext = new org.apache.spark.sql.SQLContext(sc)
  import sqlContext._

  // on init:
  registerParquetTables()

  /**
   *
   */
  private def registerParquetTables() = {
    // accounts
    SparkSQL.parquetTable2TempTable(sqlContext, ACCOUNTSPARQUETTABLE, TMPACCOUNTSTABLE)
    //
    (1 to 31).foreach{ dayOfMonth =>
      List(true, false).foreach { createJustASnapshot =>
        try {
          val currentRAMParquetPrefix = {
            if (createJustASnapshot) "%s_%d".format(mayParquetFilesPrefix, dayOfMonth)
            else "%s_%d_sample".format(mayParquetFilesPrefix, dayOfMonth)
          }
          val currentRAMParquetFile = "%s/%s.parquet".format(RAM_OUTPUT_DIR, currentRAMParquetPrefix)
          val tmpRAMTable = "%s_%s".format(TMPRAMTABLEPREFIX, currentRAMParquetPrefix)
          println("Converting from [%s] to [%s]".format(currentRAMParquetFile, tmpRAMTable))
          SparkSQL.parquetTable2TempTable(sqlContext, currentRAMParquetFile, tmpRAMTable)
        }
        catch {
          case e: Exception =>
            println(e.getMessage)
            println("NOT processing %s of day %d".format({if (createJustASnapshot) "full dump" else "sample dump"}, dayOfMonth))
        }
      }
    }
  }


  private def getAllRAMTableNames(fromDay: Int, toDay: Int, ramSnapshot: Int): List[String] = {
    (fromDay to toDay).toList.foldLeft(List[String]()) { (aList, dayOfMonth) =>
      getRAMTableName(dayOfMonth, isSnapshot = (ramSnapshot == 0)) :: aList
    }
  }

  private def getQueriesForHeadingMarkets(accountsTableName: String, fromDay: Int, toDay: Int, ramSnapshot: Int): List[String] = {
    getAllRAMTableNames(fromDay, toDay, ramSnapshot).map{ tmpRAMTable =>
      "SELECT %s.headingId, %s.directoryId, %s.keywords, %s.accountName FROM    %s              JOIN %s         ON %s.accountKey = %s.accountKey".
        format(tmpRAMTable, tmpRAMTable,    tmpRAMTable, accountsTableName,   accountsTableName, tmpRAMTable,     tmpRAMTable,     accountsTableName)
    }
  }

  private def getQueriesForMerchantsAggr(accountsTableName: String, fromDay: Int, toDay: Int, ramSnapshot: Int): List[String] = {
    getAllRAMTableNames(fromDay, toDay, ramSnapshot).map{ tmpRAMTable =>
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
  def runAggregationPerHeadingAndDirectory(fromDay: Int, toDay: Int, ramSnapshot: Int) = {

    case class headingAndDirectory(heading: Long, directory: Long)
    case class queryAndBusinessName(query: String, name: String)
    case class aggregatedResult(heading: Long, directory: Long, queriesTotal: Long, queriesByName: Long, queriesEmpty: Long, queriesNONE: Long) {
      override def toString = {
        "%d\t%d\t%d\t%d\t%d\t%d".format(heading, directory, queriesTotal, queriesByName, queriesEmpty, queriesNONE)
      }
    }

    val outputDir = "results_ypaStep0/headingMarket/%s".format(nowAsString())
    println("RESULTS FROM THIS RUN WILL BE STORED IN hdfs'[%s]".format(outputDir))

    val allData = getDataInAnRDD(getQueriesForHeadingMarkets(TMPACCOUNTSTABLE, fromDay, toDay, ramSnapshot))
    // NB: fields returned are as defined in <getQueriesForHeadingMarkets>
    val allDataPerAccount = allData.groupBy(aRowOfData => (aRowOfData.getLong(0), aRowOfData.getLong(1)))
    val keysNamesAndKeywords = allDataPerAccount.map {
      case (headAndDir, rowsWithData) =>
        (headingAndDirectory(heading = headAndDir._1, directory = headAndDir._2),
          rowsWithData.map{r => queryAndBusinessName(query = r.getString(2).replace("\"", ""), name = r.getString(3).replace("\"", ""))}.toList)
    }
    val aggregatedIndividualResults = keysNamesAndKeywords.map{ case (headAndDir, listOfResults) =>
      (headAndDir, listOfResults.map(x => (isCloseEnough(x.query, x.name), x.query.trim.isEmpty, x.query.trim.toUpperCase == "NONE")))
    }
    val aggregatedTotalResults = aggregatedIndividualResults.map{ case (headAndDir, listOfBooleanResults) =>
      val listOfResults = listOfBooleanResults.map{ indRes =>  (bool2int(indRes._1),bool2int(indRes._2),bool2int(indRes._3))}
      (headAndDir, listOfResults.length -> listOfResults.tail.foldLeft(listOfResults.head){ case (r, c) => (r._1 + c._1,r._2 + c._2,r._3 + c._3) })
    }
    val writableResults: RDD[aggregatedResult] = aggregatedTotalResults.map(r =>
      aggregatedResult(heading = r._1.heading, directory = r._1.directory, queriesTotal = r._2._1, queriesByName = r._2._2._1, queriesEmpty = r._2._2._2, queriesNONE = r._2._2._3)
    )
    writableResults.saveAsTextFile(outputDir)
    println("Output written in %s".format(outputDir))
  }


  def runAggregation(fromDay: Int, toDay: Int, ramSnapshot: Int) = {
    case class infoToReport(accountKey: Long, accountName: String, accountId: String, queriesTotal: Long, queriesByName: Long, queriesEmpty: Long, queriesNONE: Long) {
      override def toString = {
        "%d\t%s\t%s\t%d\t%d\t%d\t%d".format(accountKey, accountName, accountId, queriesTotal, queriesByName, queriesEmpty, queriesNONE)
      }
    }

    val outputDir = "results_ypaStep0/merchant/%s".format(nowAsString())
    println("RESULTS FROM THIS RUN WILL BE STORED IN hdfs'[%s]".format(outputDir))

    val allData = getDataInAnRDD(getQueriesForMerchantsAggr(TMPACCOUNTSTABLE, fromDay, toDay, ramSnapshot))
    // NB: fields returned are as defined in <getQueriesForMerchantsAggr>
    // First I group by accountKey + date ==> decomposition of event is aggregated
    case class detailedInfo(accountKey: Long, accountName: String, accountId: String, queries: List[String])
    val keysNamesAndHowManyKeywords: RDD[infoToReport] = allData.
      groupBy(aRow => (aRow.getLong(0), cleanString(aRow.getString(1)), cleanString(aRow.getString(2)), cleanString(aRow.getString(3)))).
      map{rowGrouped => val ((accountKey,accountName,accountId,theDate), allQueries) = rowGrouped; detailedInfo(accountKey, accountName, accountId, queries = allQueries.map(aQuery => aQuery(4).toString).toList)}.
      map{d =>
        infoToReport(
          accountKey = d.accountKey, accountName = d.accountName, accountId = d.accountId,
          queriesTotal = d.queries.length,
          queriesByName = d.queries.filter(aQuery => isCloseEnough(aQuery, d.accountName)).length,
          queriesEmpty = d.queries.filter(aQuery => (aQuery.trim == "")).length,
          queriesNONE = d.queries.filter(aQuery => (aQuery.trim.toUpperCase == "NONE")).length)
      }
    keysNamesAndHowManyKeywords.saveAsTextFile(outputDir)
    println("Output written in %s".format(outputDir))
  }

} // class


/*
val qe = new DataExplorer(sc)
qe.runAggregationPerHeadingAndDirectory(fromDay = 10, toDay = 12, ramSnapshot = 0)
qe.runAggregation(10, 12, 0)
 */
