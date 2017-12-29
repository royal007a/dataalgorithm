package com.zw.java.algorithm.naiveBayesClassifier

import org.apache.hadoop.io.{BytesWritable, DoubleWritable}
import org.apache.hadoop.mapred.SequenceFileInputFormat
import org.apache.spark.{SparkConf, SparkContext}

/**
  *  使用上一步得到的bayes 分类器
  */

/**
  * Naive Bayes Classifier, which classifies (using the
  * classifier built by the BuildNaiveBayesClassifier class)
  * new data.
  *
  * Now for a given X = (X1, X2, ..., Xm), we will classify it
  * by using the following data structures (built by the
  * BuildNaiveBayesClassifier class):
  *
  *     ProbabilityTable(c) = p-value where c in C
  *     ProbabilityTable(c, a) = p-value where c in C and a in A
  *
  * Therefore, given X, we will classify it as C where C in {C1, C2, ..., Ck}
  * 朴素贝叶斯原理特别简单  就是  对于给出的条件，， 求解此项出现的条件下各个类别最大的概率，， 那个大
  * 就属于那个分类
  * 假设各特征属性相互独立 p(yk | x)  = max { p(y1 | x), p(y2 | x), p(yn | x)}   p(x| yi) p(yi)  = p(yk)*p(aj| yi)
  * */
class NaiveBayesClassfier  extends Serializable{



}

object NaiveBayesClassfier {

  def main(args: Array[String]): Unit = {
    val inputPath = args(0)
    val classicInputPath = args(1)
    val classesInputPath = args(2)

    val sparkConf = new SparkConf().setAppName("naive bayes classific")
    val sc = new SparkContext(sparkConf)

    // STEP-3: read new data to be classified
    val bePredictRdd = sc.textFile(inputPath)

    // 读出分类器
//    val naiveBayesRdd = sc.textFile(classicInputPath, SequenceFileInputFormat[Tuple2[Tuple2[String, String], DoubleWritable])
   val naiveBayesRdd = sc.sequenceFile[Tuple2[String, String], DoubleWritable](classicInputPath)

    // map 与 flatMap的区别
    // // <K2,V2> JavaPairRDD<K2,V2> mapToPair(PairFunction<T,K2,V2> f)
    // Return a new RDD by applying a function to all elements of this RDD.
    val classifierRdd = naiveBayesRdd.map( r => {
     val pairs =  r._1

      val k2 = new Tuple2(pairs._1, pairs._2)
      val V2 = r._2.get()

      (k2, V2)
    }
    )

    // STEP-5: cache the classifier components, which can be used from any node in the cluster.
    val classifierMap = classifierRdd.collectAsMap()
    // 广播出去
    val broadcastClassifier = sc.broadcast(classifierMap)

    // 读出类别  广播出去
    val classesRdd = sc.textFile(classesInputPath).collect()
    //Yes  No
    val broadcastClasses = sc.broadcast(classesRdd)

    // STEP-6: classify new data
    // Now, we have Naive Bayes classifier and new data
    // Use the classifier to classify new data
    // PairFlatMapFunction<T, K, V>
    // T => Iterable<Tuple2<K, V>>
    // K = <CLASS,classification> or <attribute,classification>

    val classifierResRdd = bePredictRdd.map( r => {
      val CLASSIFIER = broadcastClassifier.value
      val CLASS = broadcastClasses.value
      // 读取出待分类的每行信息
      val attributes = r.split(",")
      var selectedClass = ""
      var maxPosterior = 0.0

      //Yes0.6 No 0.4
      for(i <- 0 until CLASS.length -1) {
        // 拿出刚分类的 大类的概率
        var posterior = CLASSIFIER.get(new Tuple2("CLASS", attributes(i))).get

        // 依次求各个属性的值 的概率情况
      //  (Rain,Hot,High,Strong)
        for( j <- 0 until attributes.length - 1) {
          // 当前分类下的概率  //   (Normal, No)  (Hot, No)  (Mild, No)  (Mild, No)  (Cool, No)
          var probability = CLASSIFIER.get(new Tuple2(attributes(j), CLASS(i)))

          if(probability == null) {
            posterior = 0.0
          } else {
            posterior *= probability.get
          }

        }

        if( selectedClass == null) {
          // computing values for the first classification
          selectedClass = CLASS(i)
          maxPosterior = posterior

        } else {
          // 选出出最大的概率 作为分类
          if(posterior > maxPosterior) {
            selectedClass = CLASS(i)
            maxPosterior = posterior
          }
        }

      }
     new Tuple2(r, selectedClass)
    })
  }
}

/*
PT={
    (Normal,No)=0.2,
    (Mild,Yes)=0.4444444444444444,
    (Normal,Yes)=0.6666666666666666,
    (Overcast,Yes)=0.4444444444444444,
    (CLASS,No)=0.35714285714285715,
    (CLASS,Yes)=0.6428571428571429,
    (Hot,Yes)=0.2222222222222222,
    (Hot,No)=0.4,
    (Cool,No)=0.2,
    (Sunny,No)=0.6,
    (High,No)=0.8,
    (Rain,No)=0.4,
    (Sunny,Yes)=0.2222222222222222,
    (Cool,Yes)=0.3333333333333333,
    (Rain,Yes)=0.3333333333333333,
    (Mild,No)=0.4,
    (High,Yes)=0.3333333333333333
   }

# hadoop fs -text /classifier.seq/part*
(Normal, No)	0.2
(Mild, Yes)	0.4444444444444444
(Normal, Yes)	0.6666666666666666
(Overcast, Yes)	0.4444444444444444
(CLASS, No)	0.35714285714285715
(CLASS, Yes)	0.6428571428571429
(Hot, Yes)	0.2222222222222222
(Hot, No)	0.4
(Cool, No)	0.2
(Sunny, No)	0.6
(High, No)	0.8
(Rain, No)	0.4
(Sunny, Yes)	0.2222222222222222
(Cool, Yes)	0.3333333333333333
(Rain, Yes)	0.3333333333333333
(Mild, No)	0.4
(High, Yes)	0.3333333333333333

# hadoop fs -cat /output/classified/part*
(Rain,Hot,High,Strong,Yes)
(Overcast,Mild,Normal,Weak,Yes)
(Sunny,Mild,Normal,Week,Yes)

*/

