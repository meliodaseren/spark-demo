package com.iii.spark101

import org.apache.spark._
import com.maxmind.geoip._
import com.vpon.wizad.etl.util._

object GenerateLocationInfo {

  def main(args: Array[String]) {

    val sc = new SparkContext()
    
    val outputPath = "hdfs://localhost/user/cloudera/spark101/audi/location_info_added"
    
    var lookupService: LookupService = null
    
    var qkGenerator: GenerateQuadKey = null
    
    val accNumBadRecords = sc.longAccumulator("accNumBadRecords")
    
    val accNumGoodRecords = sc.longAccumulator("accNumGoodRecords")

    val records = sc.textFile("hdfs://localhost/user/cloudera/spark101/audi/data/")

    def addLocationInfo(record: String) = {
      if (lookupService == null) {
        lookupService = new LookupService("./GeoIPCityap.dat",
          LookupService.GEOIP_MEMORY_CACHE | LookupService.GEOIP_CHECK_CACHE)
      }
      if (qkGenerator == null) qkGenerator = new GenerateQuadKey()

      val ip = record.split(",")(13)
      val location = lookupService.getLocation(ip)

      if (location == null) {
        accNumBadRecords.add(1)
        "-----"
      } else {
        val qk = qkGenerator.generateQuadKey(location.latitude, location.longitude)
        accNumGoodRecords.add(1)
        s"$record,${location.countryName},${location.city},${location.latitude},${location.longitude},$qk"
      }
    }    

    records.map(addLocationInfo).
            filter(_ != "-----").
            saveAsTextFile(outputPath)
    
   println(s"Number of good records: ${accNumGoodRecords.value}, Number of bad records: ${accNumBadRecords.value} ")
  }
}