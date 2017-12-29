package com.zw.java.algorithm.lineraregression

// import org.apache.spark.ml.feature.LabeledPoint
// import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.{LabeledPoint, LinearRegressionModel, LinearRegressionWithSGD}
//import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.{SparkConf, SparkContext}

object MLLinearRegression {

  def main(args: Array[String]): Unit = {
    // 创建spark 对象
    val sparkConf = new SparkConf().setAppName("linearRegression")
    val sc = new SparkContext(sparkConf)
    // 读取样本数据1
    val dataRdd = sc.textFile("")
    val example = dataRdd.map( r => {
      val parts = r.split(",")
      LabeledPoint(parts(0).toDouble, Vectors.dense(parts(1).split(' ').map(_.toDouble)))
    })
    example.cache()

    val exampleNum = example.count()

    // 读取样本数据2


    // 建立线性回归模型  并建立参数
    val numIterations = 100
    val stepSize = 1
    val miniBatchFraction = 1
    val model = LinearRegressionWithSGD.train(example, numIterations, stepSize, miniBatchFraction)

    // 权重
    print(model.weights)
    print(model.intercept)

    // 对样本进行训练
    val prediction = model.predict(example.map((_.features)))
    val predictionAndLabel = prediction.zip(example.map(_.label))
    val print_prediction = predictionAndLabel.take(50)

    for(i <- 0 to print_prediction.length -1) {
        print(print_prediction(i)._1 + " \t" + print_prediction(i)._2)
    }

    // 计算测量误差
    val loss = predictionAndLabel.map({
      case (p, 1) =>
        val err = p -1
        err * err
    }).reduce(_ + _)
    val rmse = math.sqrt(loss / exampleNum)


    // 模型保存
    val outputPath = "123"
    model.save(sc, outputPath)
    val sameModel = LinearRegressionModel.load(sc, outputPath)


  }

}
