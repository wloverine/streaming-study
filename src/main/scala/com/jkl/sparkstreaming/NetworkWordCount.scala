package com.jkl.sparkstreaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming._
import org.apache.spark.streaming.StreamingContext

object NetworkWordCount {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf() setMaster ("local[*]") setAppName ("NetworkWordCount")
    val ssc = new StreamingContext(conf, Seconds(2))

    val input = ssc.socketTextStream("localhost", 9999)
    val res = input.flatMap(_.split(" "))
      .map((_, 1))
      .reduceByKey(_ + _)
    
    res.print()

    ssc.start()
    ssc.awaitTermination()
  }
}
