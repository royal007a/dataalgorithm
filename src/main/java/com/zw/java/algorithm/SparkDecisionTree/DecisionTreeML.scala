package com.zw.java.algorithm.SparkDecisionTree

import org.apache.spark.mllib.tree.DecisionTree
import org.apache.spark.mllib.tree.model.DecisionTreeModel
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.{SparkConf, SparkContext}


object DecisionTreeML {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("decision tree")
    val sc = new SparkContext(conf)

    val data = MLUtils.loadLibSVMFile(sc, "")
    val splits = data.randomSplit(Array(0.7, 0.3))
    val (trainingData, testData) = (splits(0), splits(1))


    // 建立决策树
    // 分类个数
    val numClasses = 2
    val categoricalFeaturesInfo = Map[Int, Int]()
    val impurity = "gini"
    // 树的深度
    val maxDepth = 5
    val maxBins = 32y
    // 此处训练的为分类树  还可以训练回归树
    val model = DecisionTree.trainClassifier(trainingData, numClasses, categoricalFeaturesInfo, impurity, maxDepth, maxBins)

    // 计算误差
    val labelAndPreds = testData.map{
      point =>
        val prediction = model.predict(point.features)
        (point.label, prediction)
    }
    val print_predict = labelAndPreds.take(20)
    for(i <-  0 to print_predict.length -1) {
      println(print_predict(i)._1 + "\t" + print_predict(i)._2_)
    }

    val testErr = labelAndPreds.filter( r => r._1 != r._2).count().toDouble/ testData.count()
    print("Test error" + testErr)
    println("")

    // 模型保存
    val modelPath = ""
    model.save(sc, modelPath)
    val sameModel = DecisionTreeModel.load(sc, modelPath)







  }



}
