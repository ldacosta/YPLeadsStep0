package ypleads.common

import org.scalatest.FlatSpec
import ypleads.common.DataNomenclature.{RAM, Accounts}

class DataNomenclatureTest extends FlatSpec {

  "Accounts' Parquet Table name" should "start with the right directory" in {
    assert(Accounts.ACCOUNTSPARQUETTABLE.startsWith(Accounts.ACCOUNTS_OUTPUT_DIR))
  }

  "Source RAM Data name" should "not exist when year is wrong" in {
    List(true, false).foreach { online =>
      assert(!RAM.getSourceRAMDataName(year = 3000, month = 1, day = 1, online).isDefined)
    }
  }

  it should "not exist when month is wrong" in {
    List(true, false).foreach { online =>
      assert(!RAM.getSourceRAMDataName(year = 2014, month = -1, day = 1, online).isDefined)
    }
  }

  it should "not exist when day is wrong" in {
    List(true, false).foreach { online =>
      assert(!RAM.getSourceRAMDataName(year = 2014, month = 1, day = 32, online).isDefined)
    }
  }

  it should "be defined when parameters are sane" in {
    List(true, false).foreach { online =>
      (2011 to 2014).foreach { yyyy =>
        (1 to 12).foreach { mm =>
          (1 to 31).foreach { dd =>
            withClue(s"Failed for online = ${online}, year/mm/dd = ${yyyy}/${mm}/${dd}") {
              assert(RAM.getSourceRAMDataName(year = yyyy, month = mm, day = dd, online).isDefined)
            }
          }
        }
      }
    }
  }

  "Source RAM file" should "not exist when year is wrong" in {
    List(true, false).foreach { online =>
      assert(!RAM.getSourceFullFileName(year = 3000, month = 1, day = 1, online).isDefined)
    }
  }

 it should "not exist when month is wrong" in {
   List(true, false).foreach { online =>
     assert(!RAM.getSourceFullFileName(year = 2014, month = -1, day = 1, online).isDefined)
   }
  }

  it should "not exist when day is wrong" in {
    List(true, false).foreach { online =>
      assert(!RAM.getSourceFullFileName(year = 2014, month = 1, day = 32, online).isDefined)
    }
  }

  it should "be defined when parameters are sane" in {
    List(true, false).foreach { online =>
      (2011 to 2014).foreach { yyyy =>
        (1 to 12).foreach { mm =>
          (1 to 31).foreach { dd =>
            withClue(s"Failed for online = ${online}, year/mm/dd = ${yyyy}/${mm}/${dd}") {
              assert(RAM.getSourceFullFileName(year = yyyy, month = mm, day = dd, online).isDefined)
            }
          }
        }
      }
    }
  }

  "A RAM Table name" should "start with the right directory" in {
    List(true, false).foreach { online =>
      (2011 to 2014).foreach { yyyy =>
        (1 to 12).foreach { mm =>
          (1 to 31).foreach { dd =>
            List(true, false).foreach { isFullDay =>
              withClue(s"Failed for online = ${online}, year/mm/dd = ${yyyy}/${mm}/${dd}, fullDay = ${isFullDay}") {
                assert(RAM.getHDFSTableName(year = yyyy, month = mm, day = dd, fullDay = isFullDay, online).map { n => n.startsWith(RAM.TMPRAMTABLEPREFIX)}.getOrElse(false))
              }
            }
          }
        }
      }
    }
  }

}


