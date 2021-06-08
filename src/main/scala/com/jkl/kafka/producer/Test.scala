package com.jkl.kafka.producer

import cn.hutool.json.JSONUtil

object Test {
  def main(args: Array[String]): Unit = {
    val str="{\"orderId\":\"3902\",\"itemId\":\"005\",\"itemName\":\"爱格升支架\",\"itemPrice\":\"1980.0\",\"userName\":\"negan\",\"orderTime\":\"20210604 15:48:40.040\"}"
    val jsonObj=JSONUtil.parseObj(str)
    println(jsonObj.get("itemName"))
  }
}
