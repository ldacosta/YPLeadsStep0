package ypleads.common

import util.{ Util => Util }
import Util.Levenshtein

/**
 * Created by LDacost1 on 2014-08-11.
 */
object Base extends Serializable {

  object structures extends Serializable {
    case class RAMRow(accountKey: Long, keywords: String, date: String, headingId: Long, directoryId: Long, refererId: Long, impressionWeight: Double, clickWeight: Double, isMobile: Boolean) {
      override def toString = {
        "[accountKey: '%s', keywords: '%s', date: '%s', headingId: %d, directoryId: %d, refererId: %d, isMobile: '%s', impressions: '%s', clicks: '%s']".
          format(accountKey,        keywords,      date,     headingId,         directoryId,   refererId, { if (isMobile) "YES" else "NO" }, impressionWeight, clickWeight)
      }
    }

    case class anAccount(accountKey: Long, accountId: String, accountName: String)

  }

  object constants extends Serializable {
    val ACCOUNTS_OUTPUT_DIR = "YPA_BD"
    val ACCOUNTSPARQUETTABLE = s"${ACCOUNTS_OUTPUT_DIR}/caa_accounts.parquet"
    val RAM_OUTPUT_DIR = "YPA_RAM"
    val mayParquetFilesPrefix = "deduped_2013_05"


    val TMPRAMTABLE = "caa_ram"

  }

  object functions extends Serializable {
    // stupid thing I have to do because strings I am reading come with extra " " 's around
    def cleanString(aString: String) = aString.replace("\"","").trim

    def isCloseEnough(target: String, toMatch: String): Boolean = {
      // first, get rid of .com OR .ca at the end:
      val targetCleanedUp = {
        if (target.endsWith(".ca") || target.endsWith(".com"))
          target.substring(0, target.lastIndexOf('.'))
        else
          target
      }
      val tfTarget = targetCleanedUp.replace("-", "").replace(" ", "").replace("'", "").toUpperCase
      val tfToMatch = toMatch.replace("-", "").replace(" ", "").replace("'", "").toUpperCase
      (tfToMatch.indexOf(tfTarget) != -1) || // searched target appears somewhere in the query
        ((tfTarget.length >= 10) && (tfToMatch.indexOf(tfTarget.substring(0, tfTarget.length/2)) != -1)) || // searched target is long and at least half of it appears in the query (ex: target = "domino's pizza", query = "dominos")
        Levenshtein.distance(tfTarget, tfToMatch) <= (tfTarget.length / 3) // edit distance
    }

  }

}
