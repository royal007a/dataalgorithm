package com.zw.java.algorithm.SparkMLlibSVM

import org.apache.spark.mllib.classification.ClassificationModel
import org.apache.spark.mllib.pmml.PMMLExportable
import org.apache.spark.mllib.regression.GeneralizedLinearModel
import org.apache.spark.mllib.util.Saveable
import org.apache.spark.mllib.linalg.Vector

/**
  * svm 分类模型，
  *  每个特征的权重向量及偏置
  *  svm 分类模型包含方法 预测 保存模型  加载模型
  *  predict 跟军svm分类模型 计算每一个特征向量 每一个样本的预测值
  *
  *  weights 每个特征的权重
  *  intercept 偏置项
  */
class MySVMModel (
                 override val weights: Vector,
                 override  val intercept: Double
                 ) extends GeneralizedLinearModel(weights, intercept) with ClassificationModel with Serializable with  Saveable with PMMLExportable{
   private var threshold: Option[Double] = Some(0.0)

  /**
    * 阈值的设置，， 当预测值或者计算值 >= 阈值时结果为正 否则为负
    * @param threshold
    * @return
    */
  def setThreshold(threshold: Double): this.type  = {
    this.threshold = Some(threshold)
    this
  }

  override protected def predictPoint(dataMatrix: Vector, weightMatrix: Vector, intercept: Double): Double = {
    // y = wx + b
    var margin = weightMatrix.toArray.dot(dataMatrix.toArray)  + intercept

    threshold match {
      case Some(t) => if (margin > t) 1.0 else 0.0
      case None => margin  // 返回原始值
    }
  }


}
