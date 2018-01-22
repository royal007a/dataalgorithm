package com.zw.java.algorithm.SparkRecommendation

import org.apache.spark.{SparkConf, SparkContext}

object MLlibItemCF {

  def main(args: Array[String]): Unit = {

    // 构建spark 对象
    val sparkConf = new SparkConf().setAppName("ITEMCf")
    val sc = new SparkContext(sparkConf)

    // 读取样本数据
    val data_path = ""
    val data = sc.textFile(data_path)
    // 用户id  物品id  评分
    val userData = data.map(_.split(",")).map(f => (ItemPref(f(0), f(1), f(2).toDouble))).cache()

    // 建立模型
    val  mysimil = new MLItemSimilarity()

    // 生成物品同现相似度矩阵
    val simil_rdd1 = mysimil.Similarity(userData, "cooccurrence")

    val recommd = new MLRecommendedItem
    // 从物品相似度矩阵， 用户数据中推荐30个物品
    val recommend_rdd1 = recommd.Recommend(simil_rdd1, userData, 30)

    // 打印结果  (物品i， 物品j， 相似度)  （用户 ， 物品， 推荐值）
    println(s"物品相似度矩阵: ${simil_rdd1.count()}")
    simil_rdd1.collect().foreach( ItemSimi =>
     println(ItemSimi.itemid1 + "," + ItemSimi.itemid2 + "," + ItemSimi.similar)
    )
    println(s"用户推荐结果： ${recommend_rdd1.count()}")
    recommend_rdd1.collect().foreach( {
      UserRecomm => println(UserRecomm.userid + "," + UserRecomm.itemid + "," + UserRecomm.pref)
    })


  }

}
