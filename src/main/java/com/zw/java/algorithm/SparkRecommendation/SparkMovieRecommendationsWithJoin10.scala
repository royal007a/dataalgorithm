package com.zw.java.algorithm.SparkRecommendation

import org.apache.spark.{SparkConf, SparkContext}


/**
  * MovieRecommendations: very basic movie recommnedation...
  * 基于内容的推荐，，
  */
class SparkMovieRecommendationsWithJoin10 {

  def main(args: Array[String]): Unit = {
    if (args.size < 2) {
      println("Usage: MovieRecommendations <input-path> <output-path>")
      sys.exit(1)
    }

    val sparkConf = new SparkConf().setAppName("MovieRecommendations")
    val sc = new SparkContext(sparkConf)

    val input = args(0)
    val output = args(1)

    val usersRatings = sc.textFile(input)

    val userMovieRating = usersRatings.map( line => {
      val tokens = line.split("\\s")
      // userID  movie  rating
      (tokens(0), tokens(1), tokens(2).toInt)
    })

    // 算出每个电影的评分人数
    //  movie num
    val numberOfRatersPerMovie = userMovieRating.map(umr => (umr._2, 1)).reduceByKey(_+_)

    // ==> (key, value) (key, num)  join
    val userMovieRatingNumberOfRater = userMovieRating.map( r => (r._2, (r._1, r._3))).join(numberOfRatersPerMovie).map(tuple =>
      //  tuple (String, ((String, int), int))    //  movie   userID rating   numofrate
      (tuple._2._1._1, tuple._1, tuple._2._1._2, tuple._2._2)
    )

    // 构成 user moive List
    val groupedByUser = userMovieRatingNumberOfRater.map( f => (f._1, (f._2, f._3, f._4))).groupByKey()

    // (movie1, movie2) (rating1, numRate1 rating2, NumRate2, rp, rs1, rs2)
   val moviePairs = groupedByUser.flatMap( tuple => {
     val sorted = tuple._2.toList.sortBy(f => f._1) // list sort by movie
     val tuple7 = for {
       movie1 <- sorted
       movie2 <- sorted
       if( movie1._1 < movie2._1); // avoid duplicate
       ratingProduct = movie1._2 * movie2._2
       rating1Squared = movie1._2 * movie1._2
       rating2Squared = movie2._2 * movie2._2
     } yield  {
       ((movie1._1, movie2._1), (movie1._2, movie1._3, movie2._2, movie2._3, ratingProduct, rating1Squared, rating2Squared))
     }
     tuple7
   })

   // (movie._1, movie2._1) List()
    val moviePairsGrouped = moviePairs.groupByKey()

    val result = moviePairsGrouped.mapValues(
      itr => {
        val groupSize = itr.size
        val (rating1, numOfRaters1, rating2, numOfRaters2, ratingProduct, rating1Squared, rating2Squared) =
          itr.foldRight((List[Int](), List[Int](), List[Int](), List[Int](), List[Int](), List[Int](), List[Int]())) ((a, b) =>
            (
              a._1 :: b._1,
              a._2 :: b._2,
              a._3 :: b._3,
              a._4 :: b._4,
              a._5 :: b._5,
              a._6 :: b._6,
              a._7 :: b._7
            ))

        val dotProduct = ratingProduct.sum // sum of ratingProd
        val rating1Sum = rating1.sum // sum of rating1
        val rating2Sum = rating2.sum // sum of rating2
        val rating1NormSq = rating1Squared.sum // sum of rating1Squared
        val rating2NormSq = rating2Squared.sum // sum of rating2Squared
        val maxNumOfumRaters1 = numOfRaters1.max // max of numOfRaters1
        val maxNumOfumRaters2 = numOfRaters2.max // max of numOfRaters2


        val numerator = groupSize * dotProduct - rating1Sum * rating2Sum
        val denominator = math.sqrt(groupSize * rating1NormSq - rating1Sum * rating1Sum) *
          math.sqrt(groupSize * rating2NormSq - rating2Sum * rating2Sum)
        val pearsonCorrelation = numerator / denominator

        val cosineCorrelation = dotProduct / (math.sqrt(rating1NormSq) * math.sqrt(rating2NormSq))

        val jaccardCorrelation = groupSize.toDouble / (maxNumOfumRaters1 + maxNumOfumRaters2 - groupSize)

        // 皮尔逊  余弦相似度  杰卡德
        (pearsonCorrelation, cosineCorrelation, jaccardCorrelation)
      }
    )

    result.foreach(println)

    result.saveAsTextFile(output)

    sc.stop()



  }

}
