package com.zw.java.algorithm.lineraregression

import com.xiaomi.data.commons.hadoop.hdfs.GlobExist
import com.xiaomi.data.commons.spark.SparkMain
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.spark.SparkContext
//import org.apache.spark.ml.regression.LinearRegressionModel
//import org.apache.spark.mllib.regression.LinearRegressionModel
import org.joda.time.DateTime
import org.joda.time.format.DateTimeFormat
import org.json4s.DefaultFormats
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.regression.LinearRegressionModel

object ModelEvaluation  extends SparkMain{

  // json4s
  implicit val formats = DefaultFormats

  val DAYS_OF_LOG_LIMIT = 60



  /**
    */
  case class Config(inputLog: String = ".",
                    outputLog: String = ".",
                    day: DateTime = null,
                    debug: String = ".")

  val parser = new scopt.OptionParser[Config]("run") {
    head("miui active stats job", "")

    opt[String]("active") optional() valueName "<hdfs>" action { (x, c) =>
      c.copy(inputLog = x)
    } text "mitv active log input path"
    opt[String]("recovery") optional() valueName "<hdfs>" action { (x, c) =>
      c.copy(outputLog = x)
    } text "mitv recovery log input path"


    opt[String]("day") optional() valueName "day" action { (x, c) =>
      c.copy(day = DateTimeFormat.forPattern("yyyyMMdd").parseDateTime(x))
    } text "day"
    opt[String]("debug") optional() valueName "<file>" action { (x, c) =>
      c.copy(debug = x)
    } text "counter export to file"
  }

  /**
    * 检查所有路径是否（不）存在
    */
  def configCheck(conf: Configuration, config: Config): Unit = {
    val fs = FileSystem.get(conf)
    // 报活日志部分
    if (config.inputLog == ".") {
      assert(GlobExist.exist(fs, config.inputLog) > 0, "缺少MITV报活日志")
      //            assert(GlobExist.exist(fs, config.mitvRecoveryLog) > 0, "缺少MITV开机日志")
      //            assert(GlobExist.exist(fs, config.mitvSourceLog) > 0, "缺少MITV输入源日志")
      //            assert(GlobExist.exist(fs, config.mitvAccessLog) > 0, "缺少MITV外设日志")
      //            assert(GlobExist.exist(fs, config.mitvLocationLog) > 0, "缺少MITV位置日志")
    } else {
      assert(GlobExist.exist(fs, config.inputLog) > 0, "合并后的日志")
    }


    assert(GlobExist.exist(fs, config.outputLog) == 0, "输出路径")

  }

  def run(sc: SparkContext, config: Config): Unit = {
  //  Util.debugArguments(config)
    val inputPath = config.inputLog
    val outputPath = config.outputLog

    // 从保存路径中拿到 model
    val model = LinearRegressionModel.load(sc, outputPath)


    // 拿去testdata
    val testRdd = sc.textFile(inputPath)

    // 将rdd 装换成 label形式
    val labRdd = Util.BuildLabeledPointRDD(testRdd)

    // 拿出实际值和预测值
    val resRdd = labRdd.map( r => {
      var prediction = model.predict(r.features)
      (prediction, r.label)
    })

    // 计算误差率
    val MSERdd = resRdd.map( r => {
      Math.pow(r._1 - r._2, 2.0)
    })

   print("training Mean Squared Error = " + MSERdd)
  }

  override def process(sc: SparkContext, args: Array[String]) : Unit = {
    parser.parse(args, Config()) map {
      config => configCheck(sc.hadoopConfiguration, config)

    }

  }

}
