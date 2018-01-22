package com.zw.java.algorithm.SparkRecommendation

import breeze.numerics.sqrt
import org.apache.spark.rdd.RDD

/**
  * 用户评分
  * 用户  评分物品  评分
  */

case class ItemPref(
                   val userid: String,
                   val itemid: String,
                   val pref: Double
                   ) extends Serializable

/**
  * 用户推荐
  *  用户  推荐物品 评分
  */
case class UserRecomm(
                     val userid: String,
                     val itemid: String,
                     val pref: Double
                     ) extends Serializable

/**
  *
  * @param itemid1 物品1
  * @param itemid2 物品2
  * @param similar 相似度
  */
case class ItemSimi(
                   val itemid1: String,
                   val itemid2: String,
                   val similar: Double
                   ) extends Serializable



class MLItemSimilarity extends  Serializable {

  def Similarity(user_rdd: RDD[ItemPref], stype: String): (RDD[ItemSimi]) = {
    val simil_rdd = stype match {
      case "cooccurrence" =>
        MLItemSimilarity.CooccurrenceSimilarity(user_rdd)
      case "cosine" =>
        MLItemSimilarity.CosineSimiarity(user_rdd)
      case "euclidean" =>
        MLItemSimilarity.EuclideanDistanceSimilarity(user_rdd)
      case _ =>
        MLItemSimilarity.CooccurrenceSimilarity(user_rdd)
    }
    simil_rdd
  }
}

object MLItemSimilarity {

  /**
    * 同现相似度矩阵
    * 计算公式    w(i, j) = N(i)交N(j)/ sqrt(N(I)* N(J))
    * @param user_rdd  用户评分
    * @return 返回物品相似度  * @param itemid1 物品1
    *  物品2
    * 相似度
    */
  // 用户id  物品id  评分
  def CooccurrenceSimilarity(user_rdd: RDD[ItemPref]): RDD[ItemSimi] = {
     // 数据准备
    val user_rdd1 = user_rdd.map( f => (f.userid, f.itemid, f.pref))
    val user_rdd2 = user_rdd1.map( f=> (f._1, f._2))

    // （用户， 物品） 笛卡尔积操作 =》 物品与物品的组合
    val user_rdd3 = user_rdd2.join(user_rdd2)
    val user_rdd4 = user_rdd3.map( f => (f._2, 1))

    // （物品， 物品， 频次)
    val user_rdd5 = user_rdd4.reduceByKey((x, y) => x + y)

    // 对角矩阵  相同的物品过滤掉  （1,1）这种过滤掉
    val user_rdd6 = user_rdd5.filter(f => f._1._1 == f._1._2)

    // 非对角矩阵
    val user_rdd7 = user_rdd5.filter( f => f._1._1 != f._1._2)

    // 计算同现相似度 (物品1， 物品2， 同现频次)
    val user_rdd8 = user_rdd7.map( f => (f._1._1, (f._1._1, f._1._2, f._2))).
      join(user_rdd6.map( f => (f._1._1, f._2)))

    val user_rdd9 = user_rdd8.map( f =>
      ( /* 物品2 */f._2._1._2,
        (/* 物品1 */f._2._1._1,/* 物品2 */ f._2._1._2, /* 物品1 2 频次 */f._2._1._3,/* 同种频次 */ f._2._2)))

    val user_rdd10 = user_rdd9.join(user_rdd6.map( f=> (f._1._1, f._2) /* 物品1 频次 */))
    // 物品1 物品2       物品12频次 物品1频次               物品2频次
    val user_rdd11 = user_rdd10.map( f => (f._2._1._1, f._2._1._2, f._2._1._3, f._2._1._4, f._2._2))

    // 物品12 / 物品1  * 物品2
    val user_rdd12 = user_rdd11.map( f => (f._1, f._2, (f._3/ sqrt (f._4 * f._5))))

    // 结果返回
     user_rdd12.map( f => ItemSimi(f._1, f._2, f._3))

  }

  /**
    * 余弦相似度 矩阵计算
    * @param user_rdd
    * @return
    */
  def CosineSimiarity(user_rdd: RDD[ItemPref]): RDD[ItemSimi] ={

  }

  /**
    * 欧式距离相似度矩阵计算
    * @param user_rdd
    * @return
    */
  def EuclideanDistanceSimilarity(user_rdd: RDD[ItemPref]): (RDD[ItemSimi]) = {

  }


}
