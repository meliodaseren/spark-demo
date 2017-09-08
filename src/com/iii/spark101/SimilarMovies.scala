package com.iii.spark101

import org.apache.spark._
import org.apache.spark.SparkContext._
import scala.math.sqrt

object SimilarMovies {

  def removeDuplicates(userRatings: (Int, ((Int, Double), (Int, Double)))): Boolean = {
    val movieRating1 = userRatings._2._1
    val movieRating2 = userRatings._2._2
    
    val movie1 = movieRating1._1
    val movie2 = movieRating2._1
    
    movie1 < movie2
  }

  def makeMoviePairs(userRatings: (Int, ((Int, Double), (Int, Double)))) = {
    val movieRating1 = userRatings._2._1
    val movieRating2 = userRatings._2._2

    val movie1 = movieRating1._1
    val rating1 = movieRating1._2
    val movie2 = movieRating2._1
    val rating2 = movieRating2._2

    ((movie1, movie2), (rating1, rating2))
  }  
  
  def computeScore(ratingPairs: Iterable[(Double, Double)]): (Double, Int) = {
    var numPairs = 0
    var sum_xx = 0.0
    var sum_yy = 0.0
    var sum_xy = 0.0

    for (pair <- ratingPairs) {
      val ratingX = pair._1
      val ratingY = pair._2

      sum_xx += ratingX * ratingX
      sum_yy += ratingY * ratingY
      sum_xy += ratingX * ratingY
      numPairs += 1
    }
    
    var similarity = 0.0

    if ( (sqrt(sum_xx) * sqrt(sum_yy)) != 0) {
      similarity = sum_xy / (sqrt(sum_xx) * sqrt(sum_yy))
    }
    (similarity, numPairs)
  }

  def main(args: Array[String]): Unit = {

    val sc = new SparkContext()

    val data = sc.textFile("hdfs://localhost/user/cloudera/spark101/movies/movie_ratings")

    val userMovieRatings = data.map(l => l.split("\t")).map(l => (l(0).toInt, (l(1).toInt, l(2).toDouble)))

    val selfJoinedRatings = userMovieRatings.join(userMovieRatings)

    val distinctSelfJoinedRatings = selfJoinedRatings.filter(removeDuplicates)

    val moviePairs = distinctSelfJoinedRatings.map(makeMoviePairs)

    val moviePairRatings = moviePairs.groupByKey()

    val MoviePairWithScores = moviePairRatings.mapValues(computeScore).collect()

    while (true) {
      
      print("similarityThreshold: ")
      var similarityThreshold = scala.io.StdIn.readDouble()
      print("appearenceThreshold: ")
      var appearenceThreshold = scala.io.StdIn.readInt()
      print("movieID: ")
      var movieID = scala.io.StdIn.readInt()

      val result = MoviePairWithScores.filter(x =>
        {
          val moviePairs = x._1
          val similarity = x._2
          (moviePairs._1 == movieID || moviePairs._2 == movieID) &&
            similarity._1 > similarityThreshold && similarity._2 > appearenceThreshold
        })
        
      if (result.size != 0) {

        result.foreach(println)
      } else {
        println("Similar movies not found!")
      }
    }
  }
}