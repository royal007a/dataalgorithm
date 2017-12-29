package com.zw.java.algorithm.Markov

import java.text.SimpleDateFormat

import com.xiaomi.stats.common.model.BatchJobReport
import com.xiaomi.stats.util.AlertUtil
import org.apache.spark.{SparkConf, SparkContext}

class SparkMarkov (val jobName: String, yyyyMMdd: String) extends Serializable {

  val job = new BatchJobReport(jobName, yyyyMMdd, jobName, null, 0L, Map(), List(), Map())

  def run(sc: SparkContext, inputPath: String, resultPath: String): Unit = {
    val transaction = sc.textFile(inputPath)

    val dataForamt = new SimpleDateFormat("yyyy-MM-dd")
    // 拿到消費信息
    val customers = transaction.map( r => {
      val tokens = r.split(",")
      // (transaction_id, transaction_date)
      (tokens(0), (dataForamt.parse(tokens(2)).getTime.toLong, tokens(3).toDouble))
    })

    // 由交易id 分组
    val  coustomerGrouped = customers.groupByKey()

    // 根据交易日期排序
    val sortedBydate = coustomerGrouped.mapValues(_.toList.sortBy(_._1))

    // 得到交易序列 list 9种状态 构成9 *9 状态矩阵
    val stateSequence = sortedBydate.mapValues( line => {
      val sequence = for {
        i <- 0 until line.size - 1
        (currentDate, currentPurchase) = line(i)
        (nextDate, nextPutche) = line(i + 1)
      } yield {
        // 计算出间隔时间
        val elapsedTime = (nextDate - currentDate) / 86400000 match {
          case diff if (diff < 30) => "S" // 前后两个日期small
          case diff if(diff < 60) => "M"
          case _  => "L"
        }
        val amountRange = (currentPurchase / nextPutche) match {
          case ratio if (ratio < 0.9) => "L"
          case ratio if (ratio < 1.1) => "M"  // more or less same
          case _ => "G"
        }
        // 3 * 3 9种状态
        elapsedTime + amountRange
      }
      sequence
    })

    // 由上一步中的rdd =》 状态转移数量

    // STEP-6: Generate Markov State Transition
    //          Input is JavaPairRDD<K4, V4> pairs
    //            where K4=customerID, V4 = List<State>
    //          Output is a matrix of states {S1, S2, S3, ...}
    //
    //             | S1   S2   S3   ...
    //          ---+-----------------------
    //          S1 |    <probability-value>
    //             |
    //          S2 |
    //             |
    //          S3 |
    //             |
    //          ...|
    // 键值对 模拟矩阵


    val model = stateSequence.filter(_._2.size >= 2).flatMap(f => {
      val states = f._2

      val statsPair = for {
        i <- 0 until states.size - 1
        fromState = states(i)
        toState = states(i + 1)
      } yield {
        ((fromState, toState), 1)
      }
        statsPair
    })


    // 建立markov 模型
    val markovModel = model.reduceByKey(_ + _)

    val markovModelFormatted = markovModel.map( f => f._1._1 + "," + f._1._2 + "\t" + f._2)

    markovModelFormatted.foreach(println)
    markovModelFormatted.saveAsTextFile(resultPath)

  }

}


object SparkMarkov {

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf()
    val sc = new SparkContext(sparkConf)
    val date = args(0)
    val inputPath = args(1)
    val outputPath = args(2)

    val markovTask = new SparkMarkov("custom_data_stat", date)
    BatchJobReport.runJobWithTeam(markovTask.job, () => markovTask.run(sc,
      inputPath, outputPath), sc, "", false)
  }

}
