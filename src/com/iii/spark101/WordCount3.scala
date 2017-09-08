package com.iii.spark101
import org.apache.spark._

object WordCount3 {
 
  def main(args: Array[String]) {
    
    val sc = new SparkContext()
    
    val lines = sc.textFile("hdfs://localhost/user/cloudera/spark101/wordcount/book")
    
    val words = lines.flatMap(x => x.split("""\W+"""))
    
    val lowercaseWords = words.map(x => x.toLowerCase())
    
    val wordCounts = lowercaseWords.map(x => (x, 1)).
                                    reduceByKey( (x,y) => x + y)
    
    val wordCountsSorted = wordCounts.sortBy(_._1).
                                      collect().
                                      foreach(println)
  }
}

