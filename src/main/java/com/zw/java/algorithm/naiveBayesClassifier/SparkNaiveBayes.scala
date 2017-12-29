package com.zw.java.algorithm.naiveBayesClassifier

import org.apache.spark.mllib.classification.{NaiveBayes, NaiveBayesModel}
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.{SparkConf, SparkContext}

object SparkNaiveBayes {

  def main(args: Array[String]): Unit = {
    //
    val inputPath = args(0)
    val modelPath = args(1)
    //build spark 对象
    val sparkConf = new SparkConf().setAppName("naive bayes")
    val sc = new SparkContext(sparkConf)
    val data = sc.textFile(inputPath)

    // 读取数据 封入标签类内
    // 数据格式 类别， 特征1 特征2 特征3 特征4
    val parsedData = data.map( r => {
      val dataArr = r.split(",")
      LabeledPoint(dataArr(0).toDouble, Vectors.dense(dataArr(1).split(" ").map( _.toDouble)))
    })


    // 划分数据
    val splitsData = parsedData.randomSplit(Array(0.6, 0.4), seed = 11L)
    val training = splitsData(0)
    val test = splitsData(1)

    // 新建贝叶斯分类模型，， 并训练
    val bayesModel = NaiveBayes.train(training, lambda = 1.0, modelType = "multinomial")

    // 拿出训练处的样本 并使用测试数据测试
    val predictionAndLebel = test.map( p => (bayesModel.predict(p.features), p.label))
    val print_predict = predictionAndLebel.take(20)
    for(i <- 0 to print_predict.length - 1) {
      println(print_predict(i)._1 + " \t" + print_predict(i)._2)
    }

    // 计算精度
    val accuracy = 1.0 * predictionAndLebel.filter( x => x._1 == x._2).count() / test.count()

    // 保存模型
    bayesModel.save(sc, modelPath)
    val sameModel = NaiveBayesModel.load(sc , modelPath)


  }

}
