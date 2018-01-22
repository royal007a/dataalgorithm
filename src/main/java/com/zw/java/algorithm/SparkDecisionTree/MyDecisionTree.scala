package com.zw.java.algorithm.SparkDecisionTree

import org.apache.spark.internal.Logging
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.tree.{DecisionTree, RandomForest}
import org.apache.spark.mllib.tree.DecisionTree.train
import org.apache.spark.mllib.tree.configuration.Algo.{Algo, Classification, Regression}
import org.apache.spark.mllib.tree.configuration.QuantileStrategy.{QuantileStrategy, Sort}
import org.apache.spark.mllib.tree.configuration.Strategy
import org.apache.spark.mllib.tree.model.DecisionTreeModel
import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.tree.impurity._

class MyDecisionTree  private[spark] (private val strategy: Strategy, private val seed: Int)
  extends Serializable {

  def this(strategy: Strategy) = this(strategy, seed = 0)

  strategy.assertValid()

  def trainClassifier(
                       input: RDD[LabeledPoint],
                       numClasses: Int,
                       categoricalFeaturesInfo: Map[Int, Int],
                       impurity: String,
                       maxDepth: Int,
                       maxBins: Int): DecisionTreeModel = {
    val impurityType = Impurities.fromString(impurity)
    // 训练分类树
    train(input, Classification, impurityType, maxDepth, numClasses, maxBins, Sort,
      categoricalFeaturesInfo)
  }

  /**
    * 回归树的训练连
    * @param input
    * @param categoricalFeaturesInfo
    * @param impurity
    * @param maxDepth
    * @param maxBins
    * @return
    */
  def trainRegressor(
                      input: RDD[LabeledPoint],
                      categoricalFeaturesInfo: Map[Int, Int],
                      impurity: String,
                      maxDepth: Int,
                      maxBins: Int): DecisionTreeModel = {
    val impurityType = Impurities.fromString(impurity)
    train(input, Regression, impurityType, maxDepth, 0, maxBins, Sort, categoricalFeaturesInfo)
  }

  def train(
             input: RDD[LabeledPoint],
             algo: Algo,
             impurity: Impurity,
             maxDepth: Int,
             numClasses: Int,
             maxBins: Int,
             quantileCalculationStrategy: QuantileStrategy,
             categoricalFeaturesInfo: Map[Int, Int]): DecisionTreeModel = {
    val strategy = new Strategy(algo, impurity, maxDepth, numClasses, maxBins,
      quantileCalculationStrategy, categoricalFeaturesInfo)
    new MyDecisionTree(strategy).run(input)
  }

  def run(input: RDD[LabeledPoint]): DecisionTreeModel = {
    // 决策树调用的也是随机森林中的方法，，   不同的是 树为1棵
    val rf = new RandomForest(strategy, numTrees = 1, featureSubsetStrategy = "all", seed = seed)
    val rfModel = rf.run(input)
    rfModel.trees(0)
  }

}
