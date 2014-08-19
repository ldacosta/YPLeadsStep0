package util

import java.io.PrintWriter

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs._
import org.scalatest.FlatSpec
import util.Util._

class BaseTest extends FlatSpec {

  private def writeASampleFile(fileName: String): Boolean = {
    try {
      val output = FileSystem.get(new Configuration()).create(new Path(fileName))
      using(new PrintWriter(output)) { writer =>
        writer.write("lalala")
      }
      true
    }
    catch {
      case e: Exception => {
        println(s"Error: ${e.getMessage}")
        false
      }
    }
  }

  "HDFS File Exists" should "reject stupid files" in {
    assert(!HDFS.fileExists("lalala"))
  }

  it should "see created files" in {
    val fileName = "luis.txt"
    withClue(s"Impossible to create HDFS file ${fileName}") { assert(writeASampleFile(fileName)) }
    assert(HDFS.fileExists(fileName))
    withClue(s"Impossible to DELETE HDFS file ${fileName}") { assert(HDFS.deleteFile(fileName)) }
    withClue(s"NOT PROPERLY CLEANED AFTER (${fileName})") { assert(!HDFS.fileExists(fileName)) }
  }

  "Levenshtein distance on two identical strings" should "be 0" in {
    val s = "luis"
    assert(Levenshtein.distance(s, s) == 0)
  }

  it should "still be zero with 2 other strings" in {
    val s = "luis"
    assert(Levenshtein.distance(s, s) == 0)
  }

  "Transformation of int to string" should "work with months and days" in {
    (1 to 31).foreach { dd => assert(getIntAsString(dd, 2) == {
      if (dd < 10) ("0" + dd) else dd.toString
    })
    }
  }

  /**
   * Things are done like this and NOT with en enumeration of some kind
   *       because of https://issues.apache.org/jira/browse/SPARK-2330 and https://issues.apache.org/jira/browse/SPARK-1199
   *       which block the correct functioning of enumerations in the Spark Shell.
   */
  "Date components" should "fail for unknown components" in {
    assert(verifyDateComponent(12, what = "random struff").isDefined)
  }

  it should "always work for YEAR components" in {
    (1 to 20).foreach { _ =>
      assert(!verifyDateComponent(scala.util.Random.nextInt, what = "YY").isDefined)
    }
  }

  it should "*only* work for decent MONTH components" in {
    (1 to 12).foreach { aMonth =>
      assert(!verifyDateComponent(aMonth, what = "MM").isDefined)
    }
    (-10 to 0).foreach { aMonth =>
      assert(verifyDateComponent(aMonth, what = "MM").isDefined)
    }
    (13 to 33).foreach { aMonth =>
      assert(verifyDateComponent(aMonth, what = "MM").isDefined)
    }
  }

  it should "*only* work for decent DAY components" in {
    (1 to 31).foreach { aMonth =>
      assert(!verifyDateComponent(aMonth, what = "DD").isDefined)
    }
    (-10 to 0).foreach { aMonth =>
      assert(verifyDateComponent(aMonth, what = "DD").isDefined)
    }
    (32 to 45).foreach { aMonth =>
      assert(verifyDateComponent(aMonth, what = "DD").isDefined)
    }
  }

  it should "*only* work for decent HOUR components" in {
    (1 to 24).foreach { aMonth =>
      assert(!verifyDateComponent(aMonth, what = "hh").isDefined)
    }
    (-10 to 0).foreach { aMonth =>
      assert(verifyDateComponent(aMonth, what = "hh").isDefined)
    }
    (25 to 45).foreach { aMonth =>
      assert(verifyDateComponent(aMonth, what = "hh").isDefined)
    }
  }

  it should "*only* work for decent MINUTES components" in {
    (1 to 60).foreach { aMonth =>
      assert(!verifyDateComponent(aMonth, what = "mm").isDefined)
    }
    (-10 to 0).foreach { aMonth =>
      assert(verifyDateComponent(aMonth, what = "mm").isDefined)
    }
    (61 to 90).foreach { aMonth =>
      assert(verifyDateComponent(aMonth, what = "mm").isDefined)
    }
  }

  it should "*only* work for decent SECONDS components" in {
    (1 to 60).foreach { aMonth =>
      assert(!verifyDateComponent(aMonth, what = "ss").isDefined)
    }
    (-10 to 0).foreach { aMonth =>
      assert(verifyDateComponent(aMonth, what = "ss").isDefined)
    }
    (61 to 90).foreach { aMonth =>
      assert(verifyDateComponent(aMonth, what = "ss").isDefined)
    }
  }

}

