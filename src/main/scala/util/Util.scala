package util

import java.io.{PrintWriter, FileWriter}

import org.apache.spark.sql.SQLContext
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs._
import org.joda.time.DateTime

/**
 * Several Utilities.
 */
object Util extends Serializable {
  import scala.language.implicitConversions

  implicit def bool2int(b:Boolean) = if (b) 1 else 0

  object Date {
    def allDaysStartingIn(aDate: DateTime): Stream[DateTime] = Stream.cons(aDate, allDaysStartingIn(aDate.plusDays(1)))

    def getAllDays(fromDate: DateTime, toDate: DateTime): Stream[DateTime] = {
      Date.allDaysStartingIn(fromDate.withHourOfDay(1).withMinuteOfHour(0).withSecondOfMinute(0)).
        takeWhile(_.isBefore(toDate.withHourOfDay(1).withMinuteOfHour(0).withSecondOfMinute(1)))
    }
  }

  object HDFS {
    private lazy val conf = new Configuration()
    private lazy val fs = FileSystem.get(conf)

    def fileExists(fileName: String): Boolean = {
      try {
        fs.exists(new Path(fileName))
      }
      catch {
        case e: Exception => {
          println(s"Check of file ${fileName}: ${e.getMessage}")
          false
        }
      }
    }

    def deleteFile(fileName: String) = {
      try {
        fs.delete(new Path(fileName), false)
      }
      catch {
        case e: Exception => {
          println(s"Check of file ${fileName}: ${e.getMessage}")
          false
        }
      }
    }

  }

  /**
   *
   * @param dayOrMonth
   * @return
   */
  def getIntAsString(dayOrMonth: Int, minStringLength: Int): String = {
    val s = dayOrMonth.toString
    (1 to (minStringLength - s.length)).toList.foldLeft(""){ (r, _) => "0" + r } + s
  }

  /**
   * Returns current date as a String.
   * @param dateFormat
   * @return
   */
  def nowAsString() =
  {
    val dateFormat: java.text.SimpleDateFormat = new java.text.SimpleDateFormat("yy.MM.dd_hh.mm.ss")
    val now = java.util.Calendar.getInstance().getTime()
    dateFormat.format(now)
  }

  /**
   * Used for reading/writing to database, files, etc.
   * Code From the book "Beginning Scala"
   * http://www.amazon.com/Beginning-Scala-David-Pollak/dp/1430219890
   */
  def using[A <: {def close(): Unit}, B](param: A)(f: A => B): B =
    try { f(param) } finally { param.close() }

  def writeToFile(fileName:String, data:String) =
    using (new FileWriter(fileName)) {
      fileWriter => fileWriter.write(data)
    }

  def appendToFile(fileName:String, textData:String) =
    using (new FileWriter(fileName, true)){
      fileWriter => using (new PrintWriter(fileWriter)) {
        printWriter => printWriter.println(textData)
      }
    }


  import scala.math._

  object Levenshtein extends Serializable {
    def minimum(i1: Int, i2: Int, i3: Int)=min(min(i1, i2), i3)
    def distance(s1:String, s2:String)={
      val dist=Array.tabulate(s2.length+1, s1.length+1){(j,i)=>if(j==0) i else if (i==0) j else 0}

      for(j<-1 to s2.length; i<-1 to s1.length)
        dist(j)(i)= {
          if(s2.charAt(j-1) == s1.charAt(i-1)) dist(j-1)(i-1)
          else minimum(dist(j-1)(i)+1, dist(j)(i-1)+1, dist(j-1)(i-1)+1)
        }

      dist(s2.length)(s1.length)
    }

    def main(args: Array[String]): Unit = {
      printDistance("kitten", "sitting")
      printDistance("rosettacode", "raisethysword")
    }

    def printDistance(s1:String, s2:String)=println("%s -> %s : %d".format(s1, s2, distance(s1, s2)))
  }

  object SparkSQL extends Serializable {
    def parquetTable2TempTable(sqlContext: SQLContext, nameParquetTable: String, nameTable: String) = {
      val accountsAsParquet = sqlContext.parquetFile(nameParquetTable)
      accountsAsParquet.registerAsTable(nameTable)
    }
  }


}
