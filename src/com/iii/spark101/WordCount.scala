package com.iii.spark101
import org.apache.spark._

object WordCount {

  def main(args: Array[String]) {

    val sc = new SparkContext()

    val lines = sc.textFile("hdfs://localhost/user/cloudera/spark101/wordcount/book")

    val words = lines.flatMap(x => x.split(" "))

    val wordCounts = words.map(x => (x, 1)).
                           reduceByKey((x, y) => x + y)
    
    //wordCounts.collect().foreach(println)
    wordCounts.saveAsTextFile("hdfs://localhost/user/cloudera/spark101/wordcount/output")
  }
}


