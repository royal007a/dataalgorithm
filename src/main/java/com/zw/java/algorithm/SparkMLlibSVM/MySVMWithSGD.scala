package com.zw.java.algorithm.SparkMLlibSVM

import org.apache.spark.mllib.classification.SVMModel
import org.apache.spark.mllib.linalg
import org.apache.spark.mllib.optimization.{GradientDescent, HingeGradient, SquaredL2Updater}
import org.apache.spark.mllib.regression.{GeneralizedLinearAlgorithm, LabeledPoint}
import org.apache.spark.mllib.util.DataValidators
import org.apache.spark.rdd.RDD

/**
  * 建立线性svm 分类模型的入口，，
  * train方法通过设置训练参数进行模型训练
  */
object MySVMWithSGD {
  def train(
           input: RDD[LabeledPoint],
           numIterations: Int,
           stepSize: Double,
           regParam: Double,
           miniBatchFraction: Double,
           initialWeigths: Vector
           ): SVMModel = {
    new MySVMWithSGD(stepSize, numIterations, regParam, miniBatchFraction).run(input, initialWeigths)
  }

}


/**
  * 使用随机梯度先将法训练支持向量机模型，，
  *
  * 线性svm分类模型的run方法其实是 lr 中的run方法，， 该方法首先对样本增加偏置，， 初始化权重处理
  *  对权重进行优化处理 优化计算，， 最后获取最优权重，，
  *
  *  梯度下降法求解权重
  *  梯度计算
  *  权重更新
  */
class MySVMWithSGD private (
                           private var stepSize: Double,
                           private var numIterations: Int,
                           private var regParam: Double,
                           private var miniBatchFraction: Double
                           )  extends GeneralizedLinearAlgorithm[SVMModel] with  Serializable {

  // 基于铰链损失函数的梯度下降算法
  private val gradient = new HingeGradient()
  // 梯度更新方法 L2 正则化
  private val updater = new SquaredL2Updater()

  // 根据您梯度下降方法 梯度更新方法 新建梯度优化计算方法
  override val optimizer = new GradientDescent(gradient, updater)
     .setStepSize(stepSize)
       .setNumIterations(numIterations).setRegParam(regParam).setMiniBatchFraction(miniBatchFraction)

  override protected  val validators = List(DataValidators.binaryLabelValidator)

  /**
    * 利用默认参数构建支持向量机对象
    */
  def this() = this(1.0, 100, 0.01, 1.0)

  override protected def createModel(weights: linalg.Vector, intercept: Double): SVMModel = {
    new SVMModel(weights, intercept)
  }



}
