package com.zw.java.algorithm.SparkMLlibALS

import org.apache.spark.Logging
import org.apache.spark.ml.recommendation.ALS
import org.apache.spark.mllib.recommendation.{MatrixFactorizationModel, Rating}
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel

class MyALS private (
                      private var numUserBlocks: Int,
                      private var numProductBlocks: Int,
                      private var rank: Int,
                      private var iterations: Int,
                      private var lambda: Double,
                      private var implicitPrefs: Boolean,
                      private var alpha: Double,
                      private var seed: Long = System.nanoTime()
                    ) extends  Serializable with Logging{

  /**
    * 默认参数的als 实例
    * @return
    */
  def this() = this(-1, -1, 10, 10, 0.01, false, 1.0)

  /** If true, do alternating nonnegative least squares. */
  private var nonnegative = false

  /** storage level for user/product in/out links */
  private var intermediateRDDStorageLevel: StorageLevel = StorageLevel.MEMORY_AND_DISK
  private var finalRDDStorageLevel: StorageLevel = StorageLevel.MEMORY_AND_DISK

  /** checkpoint interval */
  private var checkpointInterval: Int = 10

  /**
    *  输入 RDD(user, product, rating)
    * @param ratings
    * @return  MatrixFactorizationModel
    */
  def run(ratings: RDD[Rating]): MatrixFactorizationModel = {
    val sc = ratings.context

    val numUserBlocks = if (this.numUserBlocks == -1) {
      math.max(sc.defaultParallelism, ratings.partitions.length / 2)
    } else {
      this.numUserBlocks
    }
    val numProductBlocks = if (this.numProductBlocks == -1) {
      math.max(sc.defaultParallelism, ratings.partitions.length / 2)
    } else {
      this.numProductBlocks
    }

    val (floatUserFactors, floatProdFactors) = ALS.train[Int](
      ratings = ratings.map(r => ALS.Rating(r.user, r.product, r.rating.toFloat)),
      rank = rank,
      numUserBlocks = numUserBlocks,
      numItemBlocks = numProductBlocks,
      maxIter = iterations,
      regParam = lambda,
      implicitPrefs = implicitPrefs,
      alpha = alpha,
      nonnegative = nonnegative,
      intermediateRDDStorageLevel = intermediateRDDStorageLevel,
      finalRDDStorageLevel = StorageLevel.NONE,
      checkpointInterval = checkpointInterval,
      seed = seed)

    val userFactors = floatUserFactors
      .mapValues(_.map(_.toDouble))
      .setName("users")
      .persist(finalRDDStorageLevel)
    val prodFactors = floatProdFactors
      .mapValues(_.map(_.toDouble))
      .setName("products")
      .persist(finalRDDStorageLevel)
    if (finalRDDStorageLevel != StorageLevel.NONE) {
      userFactors.count()
      prodFactors.count()
    }
    new MatrixFactorizationModel(rank, userFactors, prodFactors)
  }
// def run(ratings: RDD[Rating]): MatrixFactorizationModel = {
//   val sc = ratings.context
//
//   // 分区块设置
//   val numUserBlocks = if(this.numUserBlocks == -1) {
//     math.max(sc.defaultParallelism, ratings.partitions.size/2)
//   } else {
//     this.numUserBlocks
//   }
//   val numProductBlocks = if( this.numProductBlocks == -1) {
//     math.max(sc.defaultParallelism, ratings.partitions/2)
//   } else {
//     this.numProductBlocks
//   }
//
//   // 采用交替最小二乘法求解用户和物品的特征矩阵
//   val (floatUserFactors, floatProdFactors) = MyALS.train[Int](
//     ratings = ratings.map( r => MyALS.Rating(r.user, r.product, r.rating.toFloat))
//
//   )
//
//
//
//
// }

}

/**
  * 伴生对象
  */
object MyALS {

  def train(
           ratings: RDD[Rating],
           rank: Int,
           iterations: Int,
           lambda: Double,
           blocks: Int,
           seed: Long
           ) : MatrixFactorizationModel = {
    new MyALS(blocks, blocks, rank, iterations, lambda, false, 1.0, seed).run(ratings)
  }

}
