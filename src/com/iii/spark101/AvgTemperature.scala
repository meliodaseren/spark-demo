package com.iii.spark101
import org.apache.spark._

object AvgTemperature {
  
  def isGood(record: String) = {
    try { 
       val temp = record.split(",")(10).toInt; true}
    catch {
      case _: Exception => false
    }
  }
  
  def main(args: Array[String]) {

    val sc = new SparkContext()
    
    val records = sc.textFile("hdfs://localhost/user/cloudera/spark101/avg_temperature/weather")
    
    val goodRecords = records.filter(isGood)
    
    val dayTemp = goodRecords.map(record => (record.split(",")(1), record.split(",")(10).toInt))
    
    val result = dayTemp.mapValues(temp => (temp, 1)).
                                    reduceByKey((x, y) => (x._1 + y._1, x._2 + y._2)).
                                    map(x => (x._1, x._2._1/x._2._2)).
                                    collect().
                                    foreach(println)
  }
}



