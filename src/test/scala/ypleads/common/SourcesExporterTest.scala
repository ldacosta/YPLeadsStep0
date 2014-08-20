package ypleads.common

import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.joda.time.DateTime
import org.scalatest.FlatSpec
import ypleads.SourcesExporter

object SourcesExporterTest extends FlatSpec {

  val sc: SparkContext = new SparkContext(master = "local", appName = "SourcesExporterTest")
  val sqlContext: SQLContext = new org.apache.spark.sql.SQLContext(sc)

  "Saving RAM as Parquet" should "run AT MOST once when not forcing" in {
    val fromDate = (new DateTime).withYear(2013).withMonthOfYear(5).withDayOfMonth(11)
    val toDate = (new DateTime).withYear(2013).withMonthOfYear(5).withDayOfMonth(12)

    // first you export what you want
    SourcesExporter.saveAllDailyRAMsAsParquetFiles(sc, sqlContext, fromDate, toDate, snapshot = 0, force = false)
    // if you do it again, NOTHING should be done:
    assert(SourcesExporter.saveAllDailyRAMsAsParquetFiles(sc, sqlContext, fromDate, toDate, snapshot = 0, force = false).isEmpty)
  }

}


