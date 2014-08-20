package ypleads.common

import org.joda.time.DateTime
import org.scalatest.FlatSpec
import util.Util.Date
import ypleads.common.DataNomenclature.{RAM, Accounts}

class DataNomenclatureTest extends FlatSpec {

  "Accounts' Parquet Table name" should "start with the right directory" in {
    assert(Accounts.ACCOUNTSPARQUETTABLE.startsWith(Accounts.ACCOUNTS_OUTPUT_DIR))
  }

  "A RAM Table name" should "start with the right directory" in {
    val initDate = (new DateTime).withYear(2013).withMonthOfYear(1).withDayOfMonth(1)
    val endDate = (new DateTime).withYear(2013).withMonthOfYear(12).withDayOfMonth(31)
    List(true, false).foreach { online =>
      Date.getAllDays(initDate, endDate).foreach { aDate =>
        List(true, false).foreach { isFullDay =>
          withClue(s"Failed for online = ${online}, date = ${aDate}, fullDay = ${isFullDay}") {
            assert(RAM.getHDFSTableName(date = aDate, fullDay = isFullDay, online).map { n => n.startsWith(RAM.TMPRAMTABLEPREFIX)}.getOrElse(false))
          }
        }
      }
    }
  }

}


