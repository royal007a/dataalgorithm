package com.zw.java.algorithm.SparkRecommendation

import org.apache.spark.rdd.RDD

/**
  * 物品推荐计算类 设置模型参数，， 执行recommend方法 进行推荐计算，， 返回推荐物品的rdd
  *
  * 推荐计算根据物品相似度和用户评分进行推荐物品计算，，
  * 过滤用户已有物品及过滤最大推荐数量，，
  *
  * 用户推荐计算，， 根据物品相似度， 用户评分，， 指定最大推荐数量进行用户推荐
  */
/**
  *  收集用户偏好
  *  找到相似的用户与物品
  *  计算推荐
  *
  *  用户评分  喜好 加权，， 物品喜好程度了，，， 预处理 减燥  归一化处理
  *
  *  相似度计算   同现相似度
  *
  *  推荐计算  基于物品
  *
  *  相似度计算  协同推荐计算
  */
class MLRecommendedItem {

  def main(args: Array[String]): Unit = {



  }

  /**
    *
    * @param items_similar  物品相似度
    * @param user_perfer 用户评分
    * @param r_number   推荐数量
    * @return
    */
  // (物品1， 物品2， 相似度)  （用户 物品  评分  ）
  def Recommend(items_similar: RDD[ItemSimi], user_perfer: RDD[ItemPref], r_number: Int): (RDD[UserRecomm]) = {
    // 数据准备
    val rdd_app1_R1 = items_similar.map( f => (f.itemid1, f.itemid2, f.similar))
    val user_perfer1 = user_perfer.map( f => (f.userid, f.itemid, f.pref))

    // 矩阵计算  i行j列 join （物品1 （物品2， 相似度）） join（物品  （用户 评分））
    val rdd_app1_R2 = rdd_app1_R1.map( f => (f._1, (f._2, f._3))).join( user_perfer1.map(f1 => (f1._2, (f1._1, f1._3))))

    // 矩阵计算  i 行 j 列 元素相乘
    val rdd_app1_R3 = rdd_app1_R2.map( f => {
      // （用户, itemid1） (similar * pref)
      ((f._2._2._1, f._2._1._1), f._2._2._2 * f._2._1._2)})

    // 矩阵计算   （用户，物品） : 元素累加求和
    val rdd_app1_R4 = rdd_app1_R3.reduceByKey((x, y) => x + y)

    // 矩阵计算， 用户 对结果过滤已有物品  =》  (用户， （物品， 打分）)
    // 拿到用户评分所有物品 并过滤掉已有物品
    val rdd_app1_R5 = rdd_app1_R4.leftOuterJoin( // （用户， 物品） ，（ 打分， 1）
     // 已有的物品过滤  （用户， 物品）
      user_perfer1.map( f => {
      ((f._1, f._2), 1)
    })).filter(f => f._2._2.isEmpty).map(f => // 过滤掉已存在的
      //
      (f._1._1, (f._1._2, f._2._1))) // (用户， （物品， 打分）)

    // 用户 对用户结果排序 过滤
    val rdd_app1_R6 = rdd_app1_R5.groupByKey()
    val rdd_app1_R7 = rdd_app1_R6.map( f => {
      val i2 = f._2.toBuffer
      val i2_2 = i2.sortBy(_._2) // 根据物品打分排序
      if(i2_2.length > r_number)
        // 拿到前num的物品
        i2_2.remove(0, (i2_2.length - r_number))
      (f._1, i2_2.toIterable)
    })
    val rdd_app_R8 = rdd_app1_R7.flatMap( f => {
      val id2 = f._2
      for(w <- id2)
        yield (f._1, w._1, w._2)
    })

    rdd_app_R8.map( f => UserRecomm(f._1, f._2, f._3))  // 用户 推荐物品 打分
  }

  /**
    * 用户推荐计算
    *
    * @param items_similar  物品相似度
    * @param user_perfer  用户评分
    * @return  返回用户推荐物品
    */
//  def Recommend(items_similar: RDD[ItemSimi], user_perfer: RDD[ItemPref]) : (RDD[UserRecomm]) = {
//    // 数据准备
//    val rdd_app1_R1 = items_similar.map(f => (f.itemid1, f.itemid2, f.similar))
//    val user_perfer1 = user_perfer.map( f => (f.userid, f.itemid, f.pref))
//
//    //
//  }

}
