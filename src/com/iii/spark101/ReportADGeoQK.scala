package com.iii.spark101

import org.apache.spark.SparkContext
import com.vpon.wizad.etl.util.QuadkeyTemplateDB

object ReportADGeoQK {

  def main(args: Array[String]) {

    val sc = new SparkContext()

    val db = new QuadkeyTemplateDB("/home/cloudera/Desktop/spark101/audi/region_template/qk_cn.csv", "ALL")

    val dbBC = sc.broadcast(db)

    def line2Regions(record: String) = {
      val fields = record.split(",")
      val quadkey = fields.last
      val logType = fields(3)
      val adId = fields(4)
      val imei = fields(15)

      s"$adId,$logType,$imei" -> dbBC.value.lookupRegions(quadkey).toArray()
    }

    def calculateStats(values: Iterable[String]) = {
      var numberOfImp = 0
      var numberOfClick = 0
      val setOfViewers = collection.mutable.Set[String]()
      val setOfClickers = collection.mutable.Set[String]()

      for (line <- values) {
        val cols = line.split(",")
        if (cols(0) == "1") {
          numberOfImp += 1
          setOfViewers += cols(1)
        } else {
          numberOfClick += 1
          setOfClickers += cols(1)
        }
      }
      (numberOfImp, setOfViewers.size, numberOfClick, setOfClickers.size)
    }

    val records = sc.textFile("hdfs://localhost/user/cloudera/spark101/audi/location_info_added")

    val result = records.map(line2Regions).
      flatMapValues(x => x).
      map(x => { val fields = x._1.split(","); (s"${fields(0)},${x._2}", s"${fields(1)},${fields(2)}") }).
      groupByKey().
      mapValues(calculateStats)

    result.collect().foreach(println)
  }
}