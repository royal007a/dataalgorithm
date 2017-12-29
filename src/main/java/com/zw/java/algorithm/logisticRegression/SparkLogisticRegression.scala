package com.zw.java.algorithm.logisticRegression

import com.xiaomi.data.commons.spark.SparkMain
import org.apache.spark.mllib.classification.{LogisticRegressionModel, LogisticRegressionWithLBFGS}
import org.apache.spark.mllib.evaluation.MulticlassMetrics
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.{Accumulable, SparkContext}

import scala.collection.mutable


object SparkLogisticRegression extends SparkMain{


  override type CounterGroup = Accumulable[mutable.HashMap[String, Long], (String, Long)]

  def run(sc: SparkContext, inputPath: String, outputPath: String): Unit = {
    // 读取样本
    val data = MLUtils.loadLibSVMFile(sc, inputPath)

    // 将样本划分成训练和测试样本
    val splits = data.randomSplit(Array(0.6, 0.4), seed = 1l)
    val training = splits(0).cache()
    val test = splits(1)

    // 建立逻辑回归模型 训练
    val model = new LogisticRegressionWithLBFGS().setNumClasses(10).run(training)

    // 对测试样本进行测试
    // 检查预测结果是否和真的一致
    val predictionAndLabels = test.map {
      case LabeledPoint(label, features) =>
        val prediction = model.predict(features)
        (prediction, label)
    }

    val print_predict = predictionAndLabels.take(20)

    // 误差计算
    val metrics = new MulticlassMetrics(predictionAndLabels)
    val precision = metrics.precision

    // 模型保存
    model.save(sc, outputPath)
    val savedModel = LogisticRegressionModel.load(sc, outputPath)

  }

  override def process(sc: SparkContext, args: Array[String]): Unit = {
    val inputPath = args(0)
    val outputPath = args(1)
     run(sc, inputPath, outputPath)
  }
}
