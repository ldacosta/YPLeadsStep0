package ypleads.common

import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.scalatest.FlatSpec
import ypleads.SourcesExporter

object SourcesExporterTest extends FlatSpec {

  val sc: SparkContext = new SparkContext(master = "local", appName = "SourcesExporterTest")
  val sqlContext: SQLContext = new org.apache.spark.sql.SQLContext(sc)

  "Saving RAM as Parquet" should "run AT MOST once when not forcing" in {
    val fromYear = 2013
    val toYear = 2013
    val fromMonth = 5
    val toMonth = 5
    val fromDay = 11
    val toDay = 12
    // first you export what you want
    SourcesExporter.saveAllDailyRAMsAsParquetFiles(sc, sqlContext, fromYear, toYear, fromMonth, toMonth, fromDay, toDay, snapshot = 0, force = false)
    // if you do it again, NOTHING should be done:
    assert(SourcesExporter.saveAllDailyRAMsAsParquetFiles(sc, sqlContext, fromYear, toYear, fromMonth, toMonth, fromDay, toDay, snapshot = 0, force = false).isEmpty)
  }

  it should "do nothing when years are not sane" in {
    assert(SourcesExporter.saveAllDailyRAMsAsParquetFiles(sc, sqlContext, snapshot = 0, force = false, fromYear = 0, toYear = 0, fromMonth = 1, toMonth = 2, fromDay = 1, toDay = 2).isEmpty)
  }

  it should "do nothing when months are not sane" in {
    assert(SourcesExporter.saveAllDailyRAMsAsParquetFiles(sc, sqlContext, snapshot = 0, force = false, fromYear = 2013, toYear = 2013, fromMonth = 11, toMonth = 2, fromDay = 1, toDay = 2).isEmpty)
    assert(SourcesExporter.saveAllDailyRAMsAsParquetFiles(sc, sqlContext, snapshot = 0, force = false, fromYear = 2013, toYear = 2013, fromMonth = -11, toMonth = 2, fromDay = 1, toDay = 2).isEmpty)
    assert(SourcesExporter.saveAllDailyRAMsAsParquetFiles(sc, sqlContext, snapshot = 0, force = false, fromYear = 2013, toYear = 2013, fromMonth = 1, toMonth = 22, fromDay = 1, toDay = 2).isEmpty)
  }

  it should "do nothing when days are not sane" in {
    assert(SourcesExporter.saveAllDailyRAMsAsParquetFiles(sc, sqlContext, snapshot = 0, force = false, fromYear = 2013, toYear = 2013, fromMonth = 1, toMonth = 1, fromDay = -1, toDay = 2).isEmpty)
    assert(SourcesExporter.saveAllDailyRAMsAsParquetFiles(sc, sqlContext, snapshot = 0, force = false, fromYear = 2013, toYear = 2013, fromMonth = 1, toMonth = 1, fromDay = 11, toDay = 2).isEmpty)
    assert(SourcesExporter.saveAllDailyRAMsAsParquetFiles(sc, sqlContext, snapshot = 0, force = false, fromYear = 2013, toYear = 2013, fromMonth = 1, toMonth = 1, fromDay = 1, toDay = 22).isEmpty)
  }

}


