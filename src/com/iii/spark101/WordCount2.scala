package com.iii.spark101
import org.apache.spark._

object WordCount2 {

  def main(args: Array[String]) {

    val sc = new SparkContext()

    val lines = sc.textFile("hdfs://localhost/user/cloudera/spark101/wordcount/book")

    val words = lines.flatMap(x => x.split(" "))

    val wordCounts = words.countByValue()

    wordCounts.foreach(println)
  }
}