package com.zw.java.algorithm.lineraregression

import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.regression.LinearRegressionModel
import org.apache.spark.mllib.regression.LinearRegressionWithSGD
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.linalg.Vectors

/**
  * Training Data  Build Model
  * 模型生成阶段 将数据生成 key   labeledpoint 《label， features》 即为训练样本rdd
  *  * Input format:
  * <Price><,><Age><,><KM><,><FuelType1><,><FuelType2><,><HP><,><MetColor><,><Automatic><,><CC><,><Doors><,><Weight>
  *
  * where <Price> is the classification column.
  *  根据输入 常见 线性模型
  *
  *  The class CarPricePredictionBuildModel reads the training data and builds a
  *  LinearRegressionModel and saves it in HDFS.
  *  保存训练出模型
  */


object CarPricePredictionBuildModel {

  def  main(args: Array[String]): Unit = {

    val date = args(0)
    val inputPath = args(1)
    val resultPath = args(2)
    val sparkConf = new SparkConf()
    val sc = new SparkContext(sparkConf)

    val inputRdd = sc.textFile(inputPath)

    val trainData = BuildLabeledPointRDD(inputRdd)

    // ler is an interative algorithm
    trainData.cache()

    // 模型建立  设置步长 和迭代次数
    val stepSize = 0.0000000009; //
    val numerOfIrerations = 40

    // 输入 训练出模型
    // 该类是基于梯度下降 线性回归模型  平方误差 不使用正则化，，
    val  model = LinearRegressionWithSGD.train(trainData, numerOfIrerations, stepSize)

    // 保存模型
    model.save(sc, resultPath)


  }

  def BuildLabeledPointRDD(rdd: RDD[String]): RDD[LabeledPoint] = {

    val resRdd = rdd.map( r => {
      val tokens = r.split(",")
      val features = new Array[Double](tokens.length - 1)

      for (i <- 0 until features.length) {
        // 第一列是价格
        features(i) = tokens(i + 1).toDouble
      }
      var price = tokens(0).toDouble
      new LabeledPoint(price, Vectors.dense(features))

    })
    resRdd
  }

}
