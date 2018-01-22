package com.zw.java.algorithm.SparkDecisionTree

import java.io.IOException

import scala.collection.mutable
import scala.util.Random
import org.apache.spark.internal.Logging
import org.apache.spark.ml.classification.DecisionTreeClassificationModel
import org.apache.spark.ml.feature.LabeledPoint
import org.apache.spark.ml.regression.DecisionTreeRegressionModel
import org.apache.spark.ml.tree._

import org.apache.spark.ml.util.Instrumentation
import org.apache.spark.mllib.tree.configuration.{Algo => OldAlgo, Strategy => OldStrategy}
import org.apache.spark.mllib.tree.impurity.ImpurityCalculator
import org.apache.spark.mllib.tree.model.ImpurityStats
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.util.random.{SamplingUtils, XORShiftRandom}
import org.apache.spark.ml.tree.impl._
import org.apache.spark.ml.tree._

import scala.tools.nsc.doc.html.page.diagram.DiagramStats.TimeTracker
object MyRandomForestImpl {

  def run(
           input: RDD[LabeledPoint],
           strategy: OldStrategy,
           numTrees: Int,
           featureSubsetStrategy: String,
           seed: Long,
           instr: Option[Instrumentation[_]],
           parentUID: Option[String] = None): Array[DecisionTreeModel] = {
    val timer = new TimeTracker()

    timer.start("total")

    timer.start("init")

    val retaggedInput = input.retag(classOf[LabeledPoint])
    val metadata =
      DecisionTreeMetadata.buildMetadata(retaggedInput, strategy, numTrees, featureSubsetStrategy)
    instr match {
      case Some(instrumentation) =>
        instrumentation.logNumFeatures(metadata.numFeatures)
        instrumentation.logNumClasses(metadata.numClasses)
      case None =>
        logInfo("numFeatures: " + metadata.numFeatures)
        logInfo("numClasses: " + metadata.numClasses)
    }

    timer.start("findSplits")
    val splits = findSplits(retaggedInput, metadata, seed)
    timer.stop("findSplits")
    logDebug("numBins: feature: number of bins")
    logDebug(Range(0, metadata.numFeatures).map { featureIndex =>
      s"\t$featureIndex\t${metadata.numBins(featureIndex)}"
    }.mkString("\n"))




  }

  protected[tree] def findSplits(
                                  input: RDD[LabeledPoint],
                                  metadata: DecisionTreeMetadata,
                                  seed: Long): Array[Array[Split]] = {

    logDebug("isMulticlass = " + metadata.isMulticlass)

    val numFeatures = metadata.numFeatures

    // Sample the input only if there are continuous features.
    val continuousFeatures = Range(0, numFeatures).filter(metadata.isContinuous)
    val sampledInput = if (continuousFeatures.nonEmpty) {
      // Calculate the number of samples for approximate quantile calculation.
      val requiredSamples = math.max(metadata.maxBins * metadata.maxBins, 10000)
      val fraction = if (requiredSamples < metadata.numExamples) {
        requiredSamples.toDouble / metadata.numExamples
      } else {
        1.0
      }
      logDebug("fraction of data used for calculating quantiles = " + fraction)
      input.sample(withReplacement = false, fraction, new XORShiftRandom(seed).nextInt())
    } else {
      input.sparkContext.emptyRDD[LabeledPoint]
    }

    findSplitsBySorting(sampledInput, metadata, continuousFeatures)
  }

  private def findSplitsBySorting(
                                   input: RDD[LabeledPoint],
                                   metadata: DecisionTreeMetadata,
                                   continuousFeatures: IndexedSeq[Int]): Array[Array[Split]] = {

    val continuousSplits: scala.collection.Map[Int, Array[Split]] = {
      // reduce the parallelism for split computations when there are less
      // continuous features than input partitions. this prevents tasks from
      // being spun up that will definitely do no work.
      val numPartitions = math.min(continuousFeatures.length, input.partitions.length)

      input
        .flatMap(point => continuousFeatures.map(idx => (idx, point.features(idx))))
        .groupByKey(numPartitions)
        .map { case (idx, samples) =>
          val thresholds = findSplitsForContinuousFeature(samples, metadata, idx)
          val splits: Array[Split] = thresholds.map(thresh => new ContinuousSplit(idx, thresh))
          logDebug(s"featureIndex = $idx, numSplits = ${splits.length}")
          (idx, splits)
        }.collectAsMap()
    }

    val numFeatures = metadata.numFeatures
    val splits: Array[Array[Split]] = Array.tabulate(numFeatures) {
      case i if metadata.isContinuous(i) =>
        val split = continuousSplits(i)
        metadata.setNumSplits(i, split.length)
        split

      case i if metadata.isCategorical(i) && metadata.isUnordered(i) =>
        // Unordered features
        // 2^(maxFeatureValue - 1) - 1 combinations
        val featureArity = metadata.featureArity(i)
        Array.tabulate[Split](metadata.numSplits(i)) { splitIndex =>
          val categories = extractMultiClassCategories(splitIndex + 1, featureArity)
          new CategoricalSplit(i, categories.toArray, featureArity)
        }

      case i if metadata.isCategorical(i) =>
        // Ordered features
        //   Splits are constructed as needed during training.
        Array.empty[Split]
    }
    splits
  }


}
