package ypleads.common

import org.joda.time.DateTime
import util.{ Util => Util }
import Util._

/**
 * Created by LDacost1 on 2014-08-12.
 */
object DataNomenclature extends Serializable {

  object Accounts extends Serializable {
    val ACCOUNTS_OUTPUT_DIR = "YPA_BD"
    val ACCOUNTSPARQUETTABLE = s"${ACCOUNTS_OUTPUT_DIR}/caa_accounts.parquet"
    val TMPACCOUNTSTABLE = "caa_accounts"
  }

  object RAM extends Serializable {
    val SOURCE_DIR = "/source/ram"
    val PARQUET_DIR = "YPA_RAM"
    val TMPRAMTABLEPREFIX = "caa_ram"

    /**
     * Gets the "central" name of a day of data of the RAM table. By "central" name we mean the name of the initial
     * compressed CSV file without the extensions, and without the directory where it lives.
     *
     * @note Files for online are called:
     *       Files for mobile/API are called:  /source/ram/API_DTA_D_2013-05-31.csv.lzo
     * @example For year = 2013, month = 5, day = 15, online => name = deduped-2013-05-01
     *          For year = 2013, month = 5, day = 15, mobile => name = API_DTA_D_2013-05-01
     *
     * @return None if input parameters are invalid. Some with the result, otherwise.
     *
     */
    def getSourceRAMDataName(date: DateTime, online: Boolean): Option[String] = {
      // I only want to provide these files for certain years:
      val year = date.getYear
      if ((year >= 2011) && (year <= 2014)) {
        Some(s"${if (online) "deduped-" else "API_DTA_D_"}${year}-${getIntAsString(date.getMonthOfYear,2)}-${getIntAsString(date.getDayOfMonth,2)}")
      }
      else {
        println(s"We only accept years in [2011,2014]. Currently = ${year}")
        None
      }
    }

    /**
     * Gets the "central" name of a day of data of the GENERATED RAM tables.
     * @return None if input parameters are invalid. Some with the result, otherwise.
     */
    def getDstRAMDataName(date: DateTime, online: Boolean): Option[String] = {
      getSourceRAMDataName(date, online).map(srcName => srcName.replace("-", "_"))
    }

    def getSourceFullFileName(date: DateTime, online: Boolean): Option[String] = {
      getSourceRAMDataName(date, online).map(srcName => s"${SOURCE_DIR}/${srcName}.csv.lzo")
    }

    private def getParquetHDFSFileName(date: DateTime, fullDay: Boolean, withExtension: Boolean, online: Boolean): Option[String] = {
      getDstRAMDataName(date, online).map(dstName => s"${dstName}${if (fullDay) "_sample" else ""}${if (withExtension) ".parquet" else ""}")
    }

    def getParquetFullHDFSFileName(date: DateTime, fullDay: Boolean, online: Boolean): Option[String] = {
      getParquetHDFSFileName(date, fullDay, withExtension = true, online).map (fileName => s"${PARQUET_DIR}/${fileName}")
    }

    def getHDFSTableName(date: DateTime, fullDay: Boolean, online: Boolean): Option[String] = {
      getParquetHDFSFileName(date, fullDay, withExtension = false, online).map (fileName => s"${TMPRAMTABLEPREFIX}_${fileName}")
    }

  }

}
