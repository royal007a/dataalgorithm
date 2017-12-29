package com.zw.java.algorithm.naiveBayesClassifier

//import com.github.fommil.netlib.BLAS
import breeze.stats.distributions.{Bernoulli, Multinomial}
import org.apache.spark.mllib.linalg.BLAS
import org.apache.spark.{Logging, SparkException}
import org.apache.spark.mllib.classification.NaiveBayesModel
import org.apache.spark.mllib.linalg.{DenseVector, SparseVector}
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.linalg.Vector

class MyNaiveBayes private (private var lambda: Double, private var modelType: String) extends  Serializable  with Logging{

  def this(lambda: Double) = this(lambda, MyNaiveBayes.Multinomial)
  def this() = this(1.0, MyNaiveBayes.Multinomial)

  // 设置平滑参数
  def setLambda(lambda: Double) : MyNaiveBayes = {
    this.lambda = lambda
    this
  }
  // 平滑参数
  def getLambda: Double = lambda


  // 设置模型类别
  def setModelType(modelType: String): MyNaiveBayes = {
    require(MyNaiveBayes.supportedModelTypes.contains(modelType)),
    this.modelType = modelType
    this
  }

  // 模型类别
  def getModelType: String = this.modelType


  /**
    * 计算先验概率 和条件概率
    * 首先对所有样本数据进行聚合 label 为key  聚合同一个label的feature
    * 得到所有的label 统计之和，，  计算出先验概率，，
    * 根据条件概率  计算各个features 在各个label中的条件概率 theta（i）(j)
    * @param data
    * @return
    *         根据参数及输入样本数据  训练贝叶斯模型
    */

  def run(data: RDD[LabeledPoint]) : NaiveBayesModel ={

    val requireNonnegativeValues : org.apache.spark.mllib.linalg.Vector => Unit = (v: org.apache.spark.mllib.linalg.Vector) => {
      val values = v match {
        case sv: SparseVector => sv.values
        case dv: DenseVector => dv.values
      }

      if(!values.forall(_ >= 0.0)) {
        throw new SparkException("spark is exception")
      }
    }

    val requireZeroOneBernoulliValues: Vector => Unit = (v : Vector) => {
      val values = v match {
        case sv: SparseVector => sv.values
        case dv: DenseVector => dv.values
      }
      if(!values.forall(v => v == 0.0) || v == 1.0) {
        throw new SparkException("is exception")
      }
    }

    // 对每个标签进行聚合操作计算  求得每个标签对应的特征的频数
    // aggregate 对所有的样本数据进行聚合 以label 为key  聚合用一个label的features
    // aggregate 返回格式 （label，（计数，features之和））
    // 完成样本 v 到c类型的转化 (v: Vector) -> (c: (Long, DenseVector))
    val aggregateRdd = data.map(p => (p.label, p.features)).combineByKey(
    // .combineByKey[(Long, DenseVector)](
      createCombiner = (v: Vector) => {
        if(modelType == MyNaiveBayes.Bernoulli){
          requireZeroOneBernoulliValues(v)
        } else {
          requireNonnegativeValues(v)
        }
        (1L, v.copy.toDense)
      },
      mergeValue =  (c: (Long, DenseVector), v: Vector) => {
        requireNonnegativeValues(v)
        BLAS.axpy(1.0, v, c._2)
        (c._1 + 1l, c._2)
      },

      mergeCombiners = (c1: (Long, DenseVector), c2: (Long, DenseVector)) => {
        BLAS.axpy(1.0, c2._2, c1._2)
        (c1._1 + c2._1, c1._2)
      }
 ).collect()
//
//    val aggregateRdd = data.map( r => (r.label, r.features)).combineByKey[Long](
////      (1l, new DenseVector(new Array(1)))
//      1l
//    )

//    val aggregateRdd = data.map( r => (r.label, r.features)).reduceByKey(
//      (x, y) => {
//        x
//      }
//      //      (1l, new DenseVector(new Array(1)))
//       // 1l
//    )

    // 类别标签数量
    val numLables = aggregateRdd.length
    // 计算先验概率
    val pi = new Array[Double](numLables)
    // labels 类别标签列表
    val labels = new Array[Double](numLables)

    // 文档数量
    var numDocuments = 0l

    aggregateRdd.foreach {
      case (_, (n, _)) =>
        numDocuments += n
    }

    // 特征数量
    val numFeatures = aggregateRdd.head match {
      case (_,(_, v)) => v.size
    }

    // theta 各个特种在各个类别的条件概率
    val theta = Array.fill(numLables)(new Array[Double](numFeatures))

    // 通过聚合计算出 theta
    val piLogDenom = math.log(numDocuments + numLables * lambda)
    var i = 0
    aggregateRdd.foreach {
      case (label, (n, sumTermFreqs)) =>
        labels(i) = label
        pi(i) = math.log(n + lambda) - piLogDenom
        val thetaLogDenom = modelType match {
          case Multinomial.toString => math.log(sumTermFreqs.values.sum + numFeatures * lambda)
            // 多项式
          case Bernoulli.toString => math.log(n + 2.0 * lambda)

          case _ => throw new UnknownError(" illgeal modelType")
        }

        var j = 0
        while( j < numFeatures) {
          // 每个特征都拿出来统计 第i个标签下 第j个特征
          theta(i)(j) = math.log(sumTermFreqs(j) + lambda) - thetaLogDenom
          j += 1
        }
        i += 1
    }



    // 模型生成
    new NaiveBayesModel(labels, pi, theta, modelType)
  }

}

object MyNaiveBayes {

  // 多项式模型
  private[classification] val Multinomial: String = "multinomial"
  // 伯努利模型
  private[classification] val Bernoulli: String = "Bernoulli"

  // 设置模型支持的类别
  private[classification] val supportedModelTypes = Set(Multinomial, Bernoulli)

  // 训练模型
  def train(input: RDD[LabeledPoint]) : NaiveBayesModel = {
    new MyNaiveBayes().run(input)
  }

  def train(input: RDD[LabeledPoint], lambda: Double) : NaiveBayesModel = {
    new MyNaiveBayes(lambda, Multinomial).run(input)
  }

  def  train(input: RDD[LabeledPoint], lambda: Double, modelType: String): NaiveBayesModel = {
    require(supportedModelTypes.contains(modelType))
    new MyNaiveBayes(lambda, modelType).run(input)
  }



  //

}
