package util

import java.io.PrintWriter

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs._
import org.joda.time.DateTime
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

  "Days mentioned today" should "be 1" in {
    val now = DateTime.now
    assert(Date.getAllDays(fromDate = now, toDate = now).length == 1)
  }

  it should "be 1, even if we are comparing with *beginning* of day" in {
    val now = DateTime.now
    val todaysFirstSecond = now.withHourOfDay(0).withMinuteOfHour(0).withSecondOfMinute(1)
    assert(Date.getAllDays(fromDate = now, toDate = todaysFirstSecond).length == 1)
  }

  it should "be 1, even if we are looking at *end* of day" in {
    val now = DateTime.now
    val todaysLastSecond = now.withHourOfDay(23).withMinuteOfHour(59).withSecondOfMinute(59)
    assert(Date.getAllDays(fromDate = now, toDate = todaysLastSecond).length == 1)
  }

  "Days mentioned between yesterday and today" should "be 2 when time is the same" in {
    val now = DateTime.now
    val yesterdaySameTime = now.minusDays(1)
    withClue(s"From = ${now}, To = ${yesterdaySameTime}") { assert(Date.getAllDays(fromDate = yesterdaySameTime, toDate = now).length == 2) }
  }

  it should "be 2, even if we are looking at *beginning* of day" in {
    val now = DateTime.now
    val yesterdayFirstSecond = now.minusDays(1).withHourOfDay(0).withMinuteOfHour(0).withSecondOfMinute(1)
    withClue(s"From = ${now}, To = ${yesterdayFirstSecond}") { assert(Date.getAllDays(fromDate = yesterdayFirstSecond, toDate = now).length == 2) }
  }

  it should "be 2, even if we are looking at *end* of day" in {
    val now = DateTime.now
    val yesterdayLastSecond = now.minusDays(1).withHourOfDay(23).withMinuteOfHour(59).withSecondOfMinute(59)
    withClue(s"From = ${now}, To = ${yesterdayLastSecond}") { assert(Date.getAllDays(fromDate = yesterdayLastSecond, toDate = now).length == 2) }
  }

  "Days mentioned between today and yesterday (ie, going BACKWARDS)" should "be 0 when time is the same" in {
    val now = DateTime.now
    val yesterdaySameTime = now.minusDays(1)
    withClue(s"From = ${now}, To = ${yesterdaySameTime}") { assert(Date.getAllDays(fromDate = now, toDate = yesterdaySameTime).length == 0) }
  }

  it should "be 0, even if we are looking at *beginning* of day" in {
    val now = DateTime.now
    val yesterdayFirstSecond = now.minusDays(1).withHourOfDay(0).withMinuteOfHour(0).withSecondOfMinute(1)
    withClue(s"From = ${now}, To = ${yesterdayFirstSecond}") { assert(Date.getAllDays(fromDate = now, toDate = yesterdayFirstSecond).length == 0) }
  }

  it should "be 0, even if we are looking at *end* of day" in {
    val now = DateTime.now
    val yesterdayLastSecond = now.minusDays(1).withHourOfDay(23).withMinuteOfHour(59).withSecondOfMinute(59)
    withClue(s"From = ${now}, To = ${yesterdayLastSecond}") { assert(Date.getAllDays(fromDate = now, toDate = yesterdayLastSecond).length == 0) }
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

}

