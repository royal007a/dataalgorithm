package com.zw.java.algorithm.SparkRecommendation

import org.apache.spark.{SparkConf, SparkContext}

class SparkRecommendation {

  def main(args: Array[String]): Unit = {
    if(args.size < 2) {
      println("Usage: FriendRecommendation <input-path> <output-path>")
      sys.exit()
    }

    val sparkConf = new SparkConf().setAppName("friendRecommend")
    val sc = new SparkContext(sparkConf)

    val input = args(0)
    val output = args(1)

    val records = sc.textFile(input)

    val pairs = records.flatMap( line => {
      val tokens = line.split("\\s")
      val person = tokens(0).toLong
      val friends = tokens(1).split(",").map(_.toLong).toList

      // 直接朋友
      val mapperoutput = friends.map(directFriend => (person, (directFriend, -1.toLong)))

      // 可能的朋友
      val result = for {
        fi <- friends
        fj <- friends
        possibleFriend1 = (fj, person)
        possibleFriend2 = (fi, person)
        if(fi != fj)
      } yield {
        (fi, possibleFriend1) :: (fj, possibleFriend2) :: List()
      }

      mapperoutput ::: result.flatten

    })

    val  grouped = pairs.groupByKey()

    val result = grouped.mapValues( values => {
      val mutualFriends = new collection.mutable.HashMap[Long, List[Long]].empty

      values.foreach( t2 => {
        val toUser = t2._1;
        val mutualFriend = t2._2
        val alreadyFriend = (mutualFriend == -1)
        if(mutualFriends.contains(toUser)) {
          if(alreadyFriend) {
            mutualFriends.put(toUser, List.empty)
          } else if(mutualFriends.get(toUser).isDefined && mutualFriends.get(toUser).get.size > 0 && !mutualFriends.get(toUser).get.contains(mutualFriend)) {
            val existingList = mutualFriends.get(toUser).get
            mutualFriends.put(toUser, (mutualFriend :: existingList))
          }

        } else {
          if(alreadyFriend) {
            mutualFriends.put(toUser, List.empty)
          } else {
            mutualFriends.put(toUser, List(mutualFriend))
          }
        }
      })
      mutualFriends.filter( !_._2.isEmpty).toMap
    })

    result.saveAsTextFile(output)

    result.foreach( f => {
      val friends = if (f._2.isEmpty) "" else {
        val itmes =  f._2.map(tuple => (tuple._1, "(" + tuple._2.size + ": " + tuple._2.mkString("[", ",", "]") + ")")).map( g => "" + g._1 + " " + g._2)
      }

      println(s"${f._1} : ${friends}")
    })

    sc.stop()

  }

}


