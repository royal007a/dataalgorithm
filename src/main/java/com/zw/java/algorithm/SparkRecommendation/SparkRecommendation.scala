package com.zw.java.algorithm.SparkRecommendation

import java.util
import java.util.{HashMap, List, Map}

import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ArrayBuffer

class SparkRecommendation {

}

object  SparkRecommendation {

  def main(args: Array[String]): Unit = {
    val inputPath = args(0)
    val sparkConf = new SparkConf().setAppName("recommendation")
    val sc = new SparkContext(sparkConf)

    // 读入数据
    val records = sc.textFile(inputPath)

   // val mapperOutput = new ArrayBuffer[Tuple2[Long, Tuple2[Long, Long]]]()
    val pairsRdd = records.map( r => {
      val tokens = r.split("\t")
      val person = tokens(0).toLong
      val firendsAsString = tokens(1)
      val friendsTokenized = firendsAsString.split(",")
      val friends = new ArrayBuffer[Long]()
      val mapperOutput = new ArrayBuffer[Tuple2[Long, Tuple2[Long, Long]]]()

      // 直接朋友
      for(i <- friendsTokenized) {
        var toUser = i.toLong
        friends += toUser
        val directFriend = T2(toUser, -1l)
        var res = T2(person, directFriend)
        mapperOutput += res
        res
      }

      // 可能的朋友

      for{ k <- 0 to friends.length -1
           l <- k to friends.length -1
           } {
        var possibleFriend1 = T2(friends(l), person)
        mapperOutput += T2(friends(k), possibleFriend1)
        var possibleFreiend2 = T2(friends(k), person )
        mapperOutput += T2(friends(l), possibleFreiend2)

      }

//      mapperOutput.map( r => {
//        r
//      })
      mapperOutput.toList.flatten
      mapperOutput.iterator

     // mapperOutput.flatten

    })

    //
    val debug2 = pairsRdd.collect()
       debug2.foreach( r => {
      println("debug key" )
    })

    // 以key进行分组
    val grouped = pairsRdd.groupBy(grouped)

    val debug3 = grouped.collect()

    debug3.foreach( r => {
      println("debug3")
    })

    val recomendation = grouped.mapValues( r => {
      val mutualFriends: util.Map[Long, util.List[Long]] = new util.HashMap[Long, util.List[Long]]

    })










  }

   def T2(a: Long, b: Long) = new Tuple2[Long, Long](a, b)

   def T2(a: Long, b: Tuple2[Long, Long]) = new Tuple2[Long, Tuple2[Long, Long]](a, b)

}
