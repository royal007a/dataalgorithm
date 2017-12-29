package com.zw.java.algorithm.lineraregression

import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.rdd.RDD

object Util {



 def  debugArguments(args: Array[String]): Unit = {
   if(args == null || args.length == 0) {
     return;
   }
 }

  def BuildLabeledPointRDD(rdd: RDD[String]): RDD[LabeledPoint] = {

    val resRdd = rdd.map( r => {
      val tokens = r.split(",")
      val features = new Array[Double](tokens.length - 1)

      for (i <- 0 until features.length) {
        // 第一列是价格
        features(i) = tokens(i + 1).toDouble
      }
      var price = tokens(0).toDouble
      new LabeledPoint(price, Vectors.dense(features))

    })
    resRdd
  }





}
