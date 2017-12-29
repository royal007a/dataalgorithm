package com.zw.java.algorithm.lineraregression

import com.xiaomi.stats.common.model.BatchJobReport
import com.xiaomi.stats.util.AlertUtil
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.regression._
import org.slf4j.LoggerFactory

/**
  * we need to convert the categorical variables (such as FuelType)
  * to numeric variables to feed into Spark's linear regression model, because linear regression models only take numeric variables.
  *
  * @param jobName
  * @param date
  */

class CarPricePrediction(val jobName: String, date: String) extends  Serializable {

  val LOGGER = LoggerFactory.getLogger(classOf[CarPricePrediction])
  val job =
    new BatchJobReport(jobName, date, jobName, null, 0L, Map(), List(), Map())

  def run(sc: SparkContext, inputPath: String, outputPath: String): Unit = {


    val  model = LinearRegressionModel.load(sc, outputPath)

    var carPriceRdd = sc.textFile(inputPath)

     val predictRdd = carPriceRdd.map( r => {
       //  <Age><,><KM><,><FuelType1><,><FuelType2><,><HP><,><MetColor><,><Automatic><,><CC><,><Doors><,><Weight>
       val tokens = r.split(",")
       val features = new Array[Double](tokens.length)

       for (i <- 0 until features.length) {
         features(i) = tokens(i).toDouble
       }

       var carPriceBePrediction = model.predict(Vectors.dense(features))

       (r, carPriceBePrediction)
     }).collect()


    predictRdd.map( r => {
      LOGGER.error("record " + r._1)
      LOGGER.error("pricePredict " + r._2)
    })

  }

}



object CarPricePrediction {

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf()
    val sc = new SparkContext(sparkConf)
    val inputPath = args(0)
    val outputPath = args(1)
    val date = args(2)


    // 等待输入文件就绪
//    if (!AlertFailoverUtil.waitingForInputOnSuccessFile(new Configuration(), inputPath)) {
//      return null
//    }
//
//    MapReduceUtil.deleteFile(middletierResultPath)
//    MapReduceUtil.deleteFile(resultPath)
    val customMiddletierStat = new CarPricePrediction("custom_data_stat", date)
    BatchJobReport.runJobWithTeam(customMiddletierStat.job, () => customMiddletierStat.run(sc, inputPath, outputPath), sc, "", false)

  }

}
