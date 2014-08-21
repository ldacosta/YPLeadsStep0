package ypleads.common

import util.{ Util => Util }
import Util.Levenshtein

/**
 * Created by LDacost1 on 2014-08-11.
 */
object Common extends Serializable {

  object structures extends Serializable {
    case class RAMRow(accountKey: Long, keywords: CleanString, date: CleanString, headingId: Long, directoryId: Long, refererId: Long, impressionWeight: Double, clickWeight: Double, isMobile: Boolean) {
      override def toString = {
        "[accountKey: '%s', keywords: '%s', date: '%s', headingId: %d, directoryId: %d, refererId: %d, isMobile: '%s', impressions: '%s', clicks: '%s']".
          format(accountKey,        keywords,      date,     headingId,         directoryId,   refererId, { if (isMobile) "YES" else "NO" }, impressionWeight, clickWeight)
      }
    }

    case class anAccount(accountKey: Long, accountId: String, accountName: String)

    /* ********************************************************* */
    // Business of CleanString
    // TODO: move this to a saner location
    trait Wrapper[T]

    trait StringWrapper extends Wrapper[String] {
      val value: String
    }

    trait CleanString extends StringWrapper
    object CleanString {
      def apply(s:String): CleanString = { CleanStringImpl(s) }
      private case class CleanStringImpl(value: String) extends CleanString
    }

    // make it easy to go from "more general type" (String) to "more specific type" (CleanString).
    // Hard the other way around.
    implicit def string2CleanString(s: String) = CleanString(s)

    // End of CleanString
    /* ********************************************************* */
  }

  object constants extends Serializable {
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
