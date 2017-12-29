package com.zw.java.algorithm.naiveBayesClassifier

import java.util
import java.util.List

import com.xiaomi.stats.common.model.BatchJobReport
import com.xiaomi.stats.util.AlertUtil
import edu.umd.cloud9.io.pair.PairOfStrings
import org.apache.hadoop.io.DoubleWritable
import org.apache.spark.{SparkConf, SparkContext}
import org.slf4j.LoggerFactory

import scala.collection.mutable.ArrayBuffer

class BuilderNaiveBayesClassifier (val jobName: String, date: String)
  extends Serializable  {

  val LOGGER = LoggerFactory.getLogger(classOf[BuilderNaiveBayesClassifier])
  val job =
    new BatchJobReport(jobName, date, jobName, null, 0L, Map(), List(), Map())

  def run(sc: SparkContext, inputPath: String, outputPath: String): Unit = {

    val training = sc.textFile(inputPath)

    // 拿到训练集大小
    val trainingDataSize = training.count()

    // 训练数据中所有元素  创建属性关于分类的一个计数 用于计算条件概率

    // STEP-4: implement map() function to all elelments of training data
    // PairFlatMapFunction<T, K, V>
    // T => Iterable<Tuple2<K, V>>
    // K = <CLASS,classification> or <attribute,classification>


    // map 和flatMap 的区别
    // val pairsRdd = training.map( r => {
    val pairsRdd = training.flatMap( r => {
      // 将当前序列每个元素进行操作，， 结果放入新序列返回
      val tokens = r.split(",")
      // 前几个是属性  最后一个是 分类
      // tokens[0] = A1
      // tokens[1] = A2
      // ...
      // tokens[n-1] = An
      // token[n] = classification

      val classificationIndex = tokens.length -1
      val theClassficationIndex = tokens(classificationIndex)

       var buffer : Map[Map[String, String], String] = Map()
      // var buffer1 : ArrayBuffer[[String, String], Int]] = ArrayBuffer()
//      var eachRdd = for {
//        i <-  0 to (classificationIndex - 1)
//        var K = new Tuple2(tokens(i), theClassficationIndex)
//                      new Tuple2(K, 1)
////        (currentDate, currentPurchase) = line(i)
////        (nextDate, nextPutche) = line(i + 1)
//      }

      val statsPair = for {
        i <- 0 until (classificationIndex - 1)
        fromState = tokens(i)
        toState = theClassficationIndex
      } yield {
        ((fromState, toState), 1)
      }

//    val tt = for(i <-  0 to (classificationIndex - 1)) {
////   val tt = for {
////         i <-  0 to classificationIndex
//        // 拿出每一个属性及其分类
//       var K = new Tuple2(tokens(i), theClassficationIndex)
//      // var K = Map(tokens(i) -> theClassficationIndex)
//
//        var test1 = new Tuple2(K, 1)
//     //   buffer += (K -> "1")
//       // test1
//      buffer1 ++ test1
//
//       if(i == classificationIndex -1){
//         val KK = ("CLASS", theClassficationIndex)
//         (KK -> 1)
//         buffer += (K -> "1")
//       }
//
//    //test1
//      }


  //    sequence.
//      val KK = new Tuple2[String, String]("CLASS", theClassficationIndex)
    //    eachRdd
  //   buffer

     // test
      statsPair
    })



    pairsRdd.cache()
   // pairsRdd. 拿到各个key的合并值
    val pairsMergeRdd = pairsRdd.reduceByKey(_ + _)

    val pairsMergeMapRdd = pairsMergeRdd.collectAsMap()

    // STEP-6: build the classifier data structures, which will be used
    // to classify new data; need to build the following
    //   1. the Probability Table (PT)
    //   2. the Classification List (CLASSIFICATIONS)


    var PT:Map[Tuple2[String, String], Double] = new Map()

    val CLASSIFICATIONS:ArrayBuffer[String] = new ArrayBuffer[String]()

    // 遍历map
    pairsMergeMapRdd.map( r => {
      var key = r._1
      var classification = r._2
      if( key._1.equalsIgnoreCase("CLASS")) {
        PT += (key -> (classification / trainingDataSize).toDouble)
        CLASSIFICATIONS += key._2
      } else {
        var kk = ("CLASS", classification.toString)
         var count = pairsMergeMapRdd.get(kk)
        if (count == null)
          PT += (kk -> 0.0)
        //  PT.put(kk, 0.0)
        else
          PT += ( kk ->  (r._2.asInstanceOf[Double] / count.toString))
       //   PT.put(kk, entry.getValue.asInstanceOf[Double] / count.intValue.toDouble)
      }


    })

    val list = toWritableList(PT)
    val ptRdd = sc.parallelize(list)
    val classificationRdd = sc.parallelize(CLASSIFICATIONS)


   // 将这两个结果保存   save


   }


  def toWritableList(PTMap : Map[Tuple2[String, String], Double]): ArrayBuffer[Tuple2[Tuple2[String, String], DoubleWritable]] = {
    PTMap.map( r => {
     val kk =  new Tuple2(new Tuple2(r._1._1, r._1._2), new DoubleWritable(r._2))
      kk
    })
  }

}

object BuilderNaiveBayesClassifier {

  def main(args: Array[String]): Unit = {
    // step1 处理输入参数
    if(args.length < 3 ) {
      print("input premeter is error")
    }

    val inputPath = args(0)
    val outputPath = args(1)
    val date = args(1)

    val customDBDataStat = new BuilderNaiveBayesClassifier("custom_db_stat", date)

    // 创建spark 上下文
    val sparkConf = new SparkConf().setAppName("build naive bayes model")
    val sc = new SparkContext(sparkConf)
    BatchJobReport.runJobWithTeam(
      customDBDataStat.job,
      () => customDBDataStat.run(sc, inputPath, outputPath),
      sc,
      "",
      false)

  }

}


// 输出结果 的格式如下所示：
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

# cat run_build_naive_bayes_classifier.sh
#!/bin/bash
/bin/date
source /home/hadoop/conf/env_2.4.0.sh
export SPARK_HOME=/home/hadoop/spark-1.0.0
source /home/hadoop/spark_mahmoud_examples/spark_env_yarn.sh
source $SPARK_HOME/conf/spark-env.sh

# system jars:
CLASSPATH=$CLASSPATH:$HADOOP_HOME/etc/hadoop
#
jars=`find $SPARK_HOME -name '*.jar'`
for j in $jars ; do
   CLASSPATH=$CLASSPATH:$j
done

# app jar:
export MP=/home/hadoop/spark_mahmoud_examples
export CLASSPATH=$MP/mp.jar:$CLASSPATH
export CLASSPATH=$MP/commons-math3-3.0.jar:$CLASSPATH
export CLASSPATH=$MP/commons-math-2.2.jar:$CLASSPATH
export SPARK_CLASSPATH=$CLASSPATH
prog=NaiveBayesClassifierBuilder
export HADOOP_HOME=/usr/local/hadoop/hadoop-2.4.0
export SPARK_LIBRARY_PATH=$HADOOP_HOME/lib/native
export JAVA_HOME=/usr/java/jdk7
export HADOOP_CONF_DIR=$HADOOP_HOME/etc/hadoop
export YARN_CONF_DIR=$HADOOP_HOME/etc/hadoop
export MY_JAR=$MP/mp.jar
export SPARK_JAR=$MP/spark-assembly-1.0.0-hadoop2.4.0.jar
export YARN_APPLICATION_CLASSPATH=$CLASSPATH:$HADOOP_HOME/etc/hadoop
#export THEJARS=$MP/commons-math-2.2.jar,$MP/commons-math3-3.0.jar
export THEJARS=$MP/cloud9-1.3.2.jar
INPUT=/naivebayes/training_data.txt
RESOURCE_MANAGER_HOST=server100
$SPARK_HOME/bin/spark-submit --class $prog \
    --master yarn-cluster \
    --num-executors 12 \
    --driver-memory 3g \
    --executor-memory 7g \
    --executor-cores 12 \
    --jars $THEJARS \
    $MY_JAR  $INPUT $RESOURCE_MANAGER_HOST
/bin/date

# hadoop fs -ls /output/
Found 3 items
drwxr-xr-x   - hadoop root,hadoop          0 2014-08-17 21:45 /output/1
drwxr-xr-x   - hadoop root,hadoop          0 2014-08-17 21:45 /output/2
drwxr-xr-x   - hadoop root,hadoop          0 2014-08-17 21:45 /output/3

# hadoop fs -cat /output/1/part*
Sunny,Hot,High,Weak,No
Sunny,Hot,High,Strong,No
Overcast,Hot,High,Weak,Yes
Rain,Mild,High,Weak,Yes
Rain,Cool,Normal,Weak,Yes
Rain,Cool,Normal,Strong,No
Overcast,Cool,Normal,Strong,Yes
Sunny,Mild,High,Weak,No
Sunny,Cool,Normal,Weak,Yes
Rain,Mild,Normal,Weak,Yes
Sunny,Mild,Normal,Strong,Yes
Overcast,Mild,High,Strong,Yes
Overcast,Hot,Normal,Weak,Yes
Rain,Mild,High,Strong,No

# hadoop fs -cat /output/2/part*
((Sunny,No),1)
((Hot,No),1)
((High,No),1)
((CLASS,No),1)
((Sunny,No),1)
((Hot,No),1)
((High,No),1)
((CLASS,No),1)
((Overcast,Yes),1)
((Hot,Yes),1)
((High,Yes),1)
((CLASS,Yes),1)
((Rain,Yes),1)
((Mild,Yes),1)
((High,Yes),1)
((CLASS,Yes),1)
((Rain,Yes),1)
((Cool,Yes),1)
((Normal,Yes),1)
((CLASS,Yes),1)
((Rain,No),1)
((Cool,No),1)
((Normal,No),1)
((CLASS,No),1)
((Overcast,Yes),1)
((Cool,Yes),1)
((Normal,Yes),1)
((CLASS,Yes),1)
((Sunny,No),1)
((Mild,No),1)
((High,No),1)
((CLASS,No),1)
((Sunny,Yes),1)
((Cool,Yes),1)
((Normal,Yes),1)
((CLASS,Yes),1)
((Rain,Yes),1)
((Mild,Yes),1)
((Normal,Yes),1)
((CLASS,Yes),1)
((Sunny,Yes),1)
((Mild,Yes),1)
((Normal,Yes),1)
((CLASS,Yes),1)
((Overcast,Yes),1)
((Mild,Yes),1)
((High,Yes),1)
((CLASS,Yes),1)
((Overcast,Yes),1)
((Hot,Yes),1)
((Normal,Yes),1)
((CLASS,Yes),1)
((Rain,No),1)
((Mild,No),1)
((High,No),1)
((CLASS,No),1)

# hadoop fs -cat /output/3/part*
((Rain,Yes),3)
((Mild,No),2)
((Cool,No),1)
((Mild,Yes),4)
((Sunny,Yes),2)
((High,Yes),3)
((Hot,No),2)
((Sunny,No),3)
((Overcast,Yes),4)
((CLASS,No),5)
((High,No),4)
((Cool,Yes),3)
((Rain,No),2)
((Hot,Yes),2)
((CLASS,Yes),9)
((Normal,Yes),6)
((Normal,No),1)

# hadoop fs -ls /naivebayes/
Found 4 items
drwxr-xr-x   - hadoop root,hadoop          0 2014-08-17 21:45 /naivebayes/classes
-rw-r--r--   3 hadoop root,hadoop         70 2014-08-17 21:32 /naivebayes/new_data_to_be_classified.txt
drwxr-xr-x   - hadoop root,hadoop          0 2014-08-17 21:45 /naivebayes/pt
-rw-r--r--   3 hadoop root,hadoop        374 2014-08-15 23:39 /naivebayes/training_data.txt

# hadoop fs -text /naivebayes/pt/part*
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

# hadoop fs -cat /naivebayes/classes/part*
Yes
No

*/



