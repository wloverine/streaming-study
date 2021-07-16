package com.jkl.kafka.consumer

import cn.hutool.json.JSONUtil
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

import java.sql.{Connection, DriverManager}
import scala.util.Try

/**
 * 消费kafka数据，存储到docker中的mariadb
 */
object ConsumeData {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("ConsumeData").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val ssc = new StreamingContext(sc, Seconds(5))


    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> "localhost:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "auto.offset.reset" -> "earliest",
      "group.id" -> "use_a_separate_group_id_for_each_stream",
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )

    val topics = Array("order")
    val stream = KafkaUtils.createDirectStream[String, String](
      ssc,
      PreferConsistent,
      Subscribe[String, String](topics, kafkaParams)
    )

    stream.map(record => {
      Try {
        val v = record.value()
        val jsonObj = JSONUtil.parseObj(v)

        val orderId = jsonObj.getInt("orderId")
        val itemId = jsonObj.getStr("itemId")
        val itemName = jsonObj.getStr("itemName")
        val itemPrice = jsonObj.getDouble("itemPrice")
        val userName = jsonObj.getStr("userName")
        val orderTime = jsonObj.getStr("orderTime")
        (orderId, itemId, itemName, itemPrice, userName, orderTime)
      }
    })
      .filter(_.isSuccess)
      .map(_.get)
      .foreachRDD { rdd =>
        rdd.foreachPartition { partitionOfRecords =>
          val conn = createConnection()
          partitionOfRecords.foreach(record => {
            val sql =
              s"""
                 |insert into `order` (order_id,item_id,item_name,item_price,user_name,order_time) 
                 |values(${record._1},'${record._2}','${record._3}',${record._4},'${record._5}','${record._6}')
                 |""".stripMargin
            println(sql)
            conn.createStatement().execute(sql)
          })
          conn.close()
        }
      }

    ssc.start()
    ssc.awaitTermination()
    ssc.stop()
  }

  def createConnection(): Connection = {
    Class.forName("com.mysql.jdbc.Driver")
    DriverManager.getConnection("jdbc:mysql://localhost:3306/spark", "root", "jkl123")
  }
}
