package com.jkl.sparkstreaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, State, StateSpec, StreamingContext}

/**
 * 统计单词在所有输入数据中的词频，输入数据可能会一直源源不断进来
 * 每一次只统计当前批次更新的词的频次
 */
object MapWithStateWC {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf() setMaster ("local[2]") setAppName ("MapWithStateWC")
    val ssc = new StreamingContext(conf, Seconds(3))
    
    ssc.checkpoint("./ck1")

    val inputDStream = ssc.socketTextStream("localhost", 9999)

    val pairs = inputDStream.flatMap(_.split(" ")).map((_, 1)).reduceByKey(_ + _)

    val mappingFunc = (word: String, one: Option[Int], state: State[Int]) => {
      val sum = one.getOrElse(0) + state.getOption().getOrElse(0)
      state.update(sum)
      (word, sum)
    }

    val res = pairs.mapWithState(StateSpec.function(mappingFunc))
    res.print()

    ssc.start()
    ssc.awaitTermination()
  }
}
