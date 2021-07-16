package com.jkl.kafka.producer;

import cn.hutool.core.util.RandomUtil;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.joda.time.DateTime;

import java.util.Arrays;
import java.util.List;
import java.util.Properties;

/**
 * 模拟数据，生成json格式的用户下单数据
 */
public class MockData {
    //用户列表
    static List<String> user_list = Arrays.asList("david", "lucy", "draven", "negan");
    //商品列表
    static List<Item> item_list = Arrays.asList(
            new Item("001", "Dell显示器", 1980.0),
            new Item("002", "爱格升支架", 998.8),
            new Item("003", "M1 ipad pro", 6999.0),
            new Item("004", "罗技Master3", 499.0),
            new Item("005", "贝尔金扩展坞", 599.0)
    );

    public static void main(String[] args) throws InterruptedException {
        Properties properties = new Properties();
        properties.put("bootstrap.servers", "localhost:9092");
        properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);


        for (int i = 0; i < 1000; i++) {
            int order_id = i + 1;
            System.out.println("order_id:" + order_id);
            String itemId = item_list.get(RandomUtil.randomInt(0, 5)).getItemId();
            String itemName = item_list.get(RandomUtil.randomInt(0, 5)).getItemName();
            Double itemPrice = item_list.get(RandomUtil.randomInt(0, 5)).getItemPrice();
            String userName = user_list.get(RandomUtil.randomInt(0, 4));
            String orderTime = new DateTime().toString("yyyyMMdd HH:mm:ss.sss");
            String res = String.format(
                    "{\"orderId\":\"%s\",\"itemId\":\"%s\",\"itemName\":\"%s\",\"itemPrice\":\"%s\",\"userName\":\"%s\",\"orderTime\":\"%s\"}",
                    order_id, itemId, itemName, itemPrice, userName, orderTime);


            producer.send(new ProducerRecord<String, String>("order", res));
            Thread.sleep(500);

        }
        producer.close();
        System.out.println("SimpleProducer Completed.");
    }
}
