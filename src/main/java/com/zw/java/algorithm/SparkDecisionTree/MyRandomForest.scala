package com.zw.java.algorithm.SparkDecisionTree

import scala.collection.JavaConverters._
import scala.util.Try
import org.apache.spark.annotation.Since
import org.apache.spark.api.java.JavaRDD
import org.apache.spark.internal.Logging
import org.apache.spark.ml.tree.{DecisionTreeModel => NewDTModel, RandomForestParams => NewRFParams}
import org.apache.spark.ml.tree.impl.{RandomForest => NewRandomForest}
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.tree.RandomForest
import org.apache.spark.mllib.tree.configuration.Algo._
import org.apache.spark.mllib.tree.configuration.QuantileStrategy._
import org.apache.spark.mllib.tree.configuration.Strategy
import org.apache.spark.mllib.tree.impurity.Impurities
import org.apache.spark.mllib.tree.impurity.Impurities._
import org.apache.spark.mllib.tree.model._
import org.apache.spark.rdd.RDD
import org.apache.spark.util.Utils
import org.apache.spark.util.Utils._

class MyRandomForest (
                       private val strategy: Strategy,
                       private val numTrees: Int,
                       featureSubsetStrategy: String,
                       private val seed: Int)
  extends Serializable with Logging {

  def run(input: RDD[LabeledPoint]): RandomForestModel = {
    val trees: Array[NewDTModel] = NewRandomForest.run(input.map(_.asML), strategy, numTrees,
      featureSubsetStrategy, seed.toLong, None)
    new RandomForestModel(strategy.algo, trees.map(_.toOld))
  }

}


object RandomForest extends Serializable with Logging {


  def trainClassifier(
                       input: RDD[LabeledPoint],
                       strategy: Strategy,
                       numTrees: Int,
                       featureSubsetStrategy: String,
                       seed: Int): RandomForestModel = {
    require(strategy.algo == Classification,
      s"RandomForest.trainClassifier given Strategy with invalid algo: ${strategy.algo}")
    val rf = new MyRandomForest(strategy, numTrees, featureSubsetStrategy, seed)
    rf.run(input)
  }

  def trainClassifier(
                       input: RDD[LabeledPoint],
                       numClasses: Int,
                       categoricalFeaturesInfo: Map[Int, Int],
                       numTrees: Int,
                       featureSubsetStrategy: String,
                       impurity: String,
                       maxDepth: Int,
                       maxBins: Int,
                       seed: Int = Utils.random.nextInt()): RandomForestModel = {
    val impurityType = Impurities.fromString(impurity)
    val strategy = new Strategy(Classification, impurityType, maxDepth,
      numClasses, maxBins, Sort, categoricalFeaturesInfo)
    trainClassifier(input, strategy, numTrees, featureSubsetStrategy, seed)
  }


  def trainRegressor(
                      input: RDD[LabeledPoint],
                      strategy: Strategy,
                      numTrees: Int,
                      featureSubsetStrategy: String,
                      seed: Int): RandomForestModel = {
    require(strategy.algo == Regression,
      s"RandomForest.trainRegressor given Strategy with invalid algo: ${strategy.algo}")
    val rf = new MyRandomForest(strategy, numTrees, featureSubsetStrategy, seed)
    rf.run(input)
  }






}
