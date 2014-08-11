package ypleads

import common.Base.{ structures => structures }
import common.Base.{ constants => constants }
import common.Base.{ functions => functions }

import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import scala.util.control.Exception._
import structures._
import functions._
import constants._

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

    val maySourceOnlineFilesPrefix = "deduped-2013-05"
    (1 to 31).toList.foldLeft(List[String]()) { (currentList, dayOfMonth) =>
      try {
        val sourceFileNamePrefix = "%s-%s".format(maySourceOnlineFilesPrefix, dayOfMonth)
        val sourceFileName = "/source/ram/%s.csv.lzo".format(sourceFileNamePrefix)
        val outputNamePrefix = sourceFileNamePrefix.replace("-", "_") // "deduped_2013_05_31"
        val ONEDAYRAMPARQUETFILE = "%s/%s.parquet".format(RAM_OUTPUT_DIR, outputNamePrefix)
        val ONEDAYRAMPARQUETFILESAMPLE = "%s/%s_sample.parquet".format(RAM_OUTPUT_DIR, outputNamePrefix)

        val ramForOneDay = sc.newAPIHadoopFile(sourceFileName, classOf[com.hadoop.mapreduce.LzoTextInputFormat], classOf[org.apache.hadoop.io.LongWritable],classOf[org.apache.hadoop.io.Text])
        val (dataSet, nameOfTable) =
          (
            snapshot match {
              case 0 => (ramForOneDay.sample(false, 0.001, 1), ONEDAYRAMPARQUETFILESAMPLE) // 0.00001 ==> about 400 records
              case 1 => (ramForOneDay, ONEDAYRAMPARQUETFILE)
            }
            /*
            snapshot match {
              case COUPLE_OF_HOURS => (ramForOneDay.sample(false, 0.001, 1), ONEDAYRAMTABLESAMPLE) // 0.00001 ==> about 400 records
              case DAY => (ramForOneDay, ONEDAYRAMTABLE)
            }
            */
            )


        val xx = dataSet.map{case(k,actualTxt) => actualTxt}.
          map(_.toString).
          map(actualTxt => (actualTxt, actualTxt.split(","))).
          flatMap{ case (actualTxt, asStringArr) =>
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
      }
      catch {
        case e: Exception => currentList
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
    accounts.saveAsParquetFile(ACCOUNTSPARQUETTABLE)
    ACCOUNTSPARQUETTABLE
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
