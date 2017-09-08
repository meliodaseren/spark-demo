package com.iii.spark101
import org.apache.spark._

object AvgTemperature2 {
  
  def isGood(record: String) = {
    try { val temp = record.split(",")(10).toInt; true}
    catch {
      case _: Exception => false
    }
  }
  
  def main(args: Array[String]) {

    val sc = new SparkContext()
    
    val records = sc.textFile("hdfs://localhost/user/cloudera/spark101/avg_temperature/weather")
    val goodRecords = records.filter(isGood)
    val dayTemp = goodRecords.map(record => (record.split(",")(1), record.split(",")(10).toInt))
    
    val result = dayTemp.combineByKey((value: Int) => (value, 1),
                                      (acc: (Int, Int), value) => (acc._1 + value, acc._2 + 1),
                                      (acc1: (Int, Int), acc2: (Int, Int)) => (acc1._1 + acc2._1, acc2._2 + acc2._2)).
                                      map(x => (x._1, x._2._1/x._2._2)).
                                      collect().
                                      foreach(println)
  }
}

