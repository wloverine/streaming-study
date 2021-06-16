package com.jkl.sparkstreaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

object WindowWC {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf() setMaster ("local[2]") setAppName ("MapWithStateWC")
    val ssc = new StreamingContext(conf, Seconds(3))

    //统计过去9s的词频，每6s刷新一次数据
    ssc.socketTextStream("localhost", 9999)
      .flatMap(_.split(" "))
      .map((_, 1))
      .reduceByKeyAndWindow((a: Int, b: Int) => a + b, Seconds(9), Seconds(6))
      .print()

    ssc.start()
    ssc.awaitTermination()
  }
}
