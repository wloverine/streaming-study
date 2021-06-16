package com.jkl.sparkstreaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * 统计单词在所有输入数据中的词频，输入数据可能会一直源源不断进来
 * 每一次都会统计所有词的出现频次
 */
object UpdateStateByKeyWC {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf() setMaster "local[*]" setAppName "UpdateStateByKeyWC"
    val ssc = new StreamingContext(sparkConf, Seconds(3))
    ssc.checkpoint("./ck")

    val inputDStream = ssc.socketTextStream("localhost", 9999)

    val pairDStream = inputDStream.flatMap(_.split(" ")).map((_, 1))

    val runningCounts=pairDStream.updateStateByKey((seq: Seq[Int], newValue: Option[Int]) => {
      val sum = seq.sum
      Option(sum + newValue.getOrElse(0))
    })
    
    runningCounts.print()

    ssc.start()
    ssc.awaitTermination()
  }
}
