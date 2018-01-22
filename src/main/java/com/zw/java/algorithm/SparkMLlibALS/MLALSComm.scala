package com.zw.java.algorithm.SparkMLlibALS

import org.apache.spark.mllib.recommendation.{ALS, MatrixFactorizationModel, Rating}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * 输入 用户 物品 评分
  * 输出  用户 物品 评分  预测结果
  */
object MLALSComm {

  // 构建对象
  val  conf = new SparkConf().setAppName("ALS")
  val sc = new SparkContext(conf)

  // 读取样本数据
  val data = sc.textFile("")
  val rating = data.map( _.split(",") match {
    case Array(user, item, rate) =>
      Rating(user.toInt, item.toInt, rate.toDouble)
  })

  // 建立模型
  val rank = 10
  val numIterations = 20
  val model = MyALS.train(rating, rank, numIterations, 0.01, 1, 1.0.toLong)

  // 预测结果
  val usersProducts = rating.map{
    case Rating(user, product, rate) =>
  (user, product)
  }
  val predictions = model.predict(usersProducts).map {
    case Rating(user, product, rate) =>
      ((user, product), rate)
  }
  val ratesAndPreds = rating.map{
    case Rating(user, product, rate) =>
      ((user, product), rate)
  }.join(predictions)
  val MSE = ratesAndPreds.map{
    case ((user, product), (r1, r2)) =>
      val err = (r1 - r2)
      err * err
  }.mean()

  println("mean squared error = " + MSE)

  // 保存 加载模型
  val modelPath = ""
  model.save(sc, "mymodel")
  val sameModel = MatrixFactorizationModel.load(sc, "mymodel")


}
