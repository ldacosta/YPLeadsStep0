package ypleads

import common.{ Common => Common }
import common.DataNomenclature._

import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import util.{ Util => Util }
import Util._
import scala.util.control.Exception._
import Common.structures._
import Common.functions._

/**
 * Created by LDacost1 on 2014-08-11.
 */
object SourcesExporter extends Serializable {


  /*
  object RAMSnapshot extends Enumeration {
    type RAMSnapshot = Value
    val COUPLE_OF_HOURS, DAY = Value
  }
  import RAMSnapshot._
  */

  // 0: COUPLE_OF_HOURS, 1: DAY
  def saveAllDailyRAMsAsParquetFiles(sc: SparkContext, sqlContext: SQLContext, snapshot: Int): List[String] = { // RAMSnapshot): String = {
    import sqlContext._

    List(true, false).foldLeft(List[String]()) { (currentList, online) =>
      (1 to 31).toList.foldLeft(currentList) { (currentList, dayOfMonth) =>
        try {
          RAM.getSourceFullFileName(year = 2013, month = 5, day = dayOfMonth, online).map { sourceFileName =>
            RAM.getParquetFullHDFSFileName(year = 2013, month = 5, dayOfMonth, fullDay = (snapshot == 1), online).map { nameOfTable =>
              val ramForOneDay = sc.newAPIHadoopFile(sourceFileName, classOf[com.hadoop.mapreduce.LzoTextInputFormat], classOf[org.apache.hadoop.io.LongWritable], classOf[org.apache.hadoop.io.Text])
              val dataSet =
                (
                  snapshot match {
                    case 0 => ramForOneDay.sample(false, 0.001, 1) // 0.00001 ==> about 400 records
                    case 1 => ramForOneDay
                  }
                  )
              val xx = dataSet.map { case (k, actualTxt) => actualTxt}.
                map(_.toString).
                map(actualTxt => (actualTxt, actualTxt.split(","))).
                flatMap { case (actualTxt, asStringArr) =>
                try {
                  Some(RAMRow(
                    accountKey = cleanString(asStringArr(2)).toLong,
                    keywords = cleanString(asStringArr(17)),
                    date = cleanString(asStringArr(3)),
                    headingId = cleanString(asStringArr(4)).toLong,
                    directoryId = cleanString(asStringArr(5)).toLong,
                    refererId = cleanString(asStringArr(7)).toLong,
                    impressionWeight = (catching(classOf[Exception]) opt cleanString(asStringArr(12)).toDouble).getOrElse(0.0),
                    clickWeight = (catching(classOf[Exception]) opt cleanString(asStringArr(13)).toDouble).getOrElse(0.0),
                    isMobile = cleanString(asStringArr(15)).toLong == 1
                  ))
                }
                catch {
                  case e: Exception =>
                    println(actualTxt)
                    println("Impossible to fetch a row from RAM: array of size %d. msg is this: %s; [2] = %s, [17] = %s, [3] = %s".format(asStringArr.length, e.getMessage, asStringArr(2), asStringArr(17), asStringArr(3)))
                    None
                }
              }
              xx.saveAsParquetFile(nameOfTable)
              nameOfTable :: currentList
            }.getOrElse(currentList)
          }.getOrElse(currentList)
        }
        catch {
          case e: Exception => currentList
        }
      }
    }
  }

  /**
   * Saves 'accounts' table into a Parquet table
   *
   * @param sc
   * @param sqlContext
   * @return the name of the Parquet table that was saved
   */
  def saveAccountsAsParquetFile(sc: SparkContext, sqlContext: SQLContext): String = {
    import sqlContext._

    val ACCOUNT_FIELDS_SEP = ";"

    val accounts = sc.textFile("/source/exp_account.csv").
      map(s => (s, s.split(ACCOUNT_FIELDS_SEP))).
      flatMap { case (s, a) =>
      try {
        val vertical = a(4).trim.toUpperCase.replace("\"","")
        if ((vertical != "MERCHANT") && (!vertical.isEmpty) && (vertical != "CLIENT")) {
          println("I THINK WE SHOULD IGNORE THIS ONE (as VERTICAL == %s)\n%s".format(a(4), s))
          None
        }
        else
          Some(anAccount(accountKey = a(0).trim.toLong, accountId = a(2), accountName = a(3)))
      }
      catch {
        case e: Exception =>
          println("Error processing array size %d".format(a.length))
          None
      }
    }
    accounts.saveAsParquetFile(Accounts.ACCOUNTSPARQUETTABLE)
    Accounts.ACCOUNTSPARQUETTABLE
  }

  /**
   *
   * @param sc
   * @param ramSnapshot
   * @return Returns (ramParquetFileName, accountsParquetFileName)
   */
  // 0: COUPLE_OF_HOURS, 1: DAY
  def saveAccountsAndRAMAsParquetFiles(sc: SparkContext, sqlContext: SQLContext, ramSnapshot: Int) = {

    println("*******************************************************")
    println("Daily RAMS:")
    saveAllDailyRAMsAsParquetFiles(sc, sqlContext, ramSnapshot).foreach(fileName => println(fileName))
    println("*******************************************************")
    println("Accounts: %s".format(saveAccountsAsParquetFile(sc, sqlContext)))

    //
    // val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    // import sqlContext._
    // val (ramParquetFileName, accountsParquetFileName) = saveAccountsAndRAMAsParquetFile(sc, sqlContext, ramSnapshot: Int) // 0: COUPLE_OF_HOURS, 1: DAY

    // test:
    // sql("SELECT * FROM %s JOIN %s ON %s.accountKey = %s.accountKey".format(ramTableName, accountsTableName, ramTableName, accountsTableName)).count
    /************************************************************************/

    // THEN ==>
    /************************************************************************/
    // runAggregation(sqlContext, accountsTableName, ramTableName)

  }

}
