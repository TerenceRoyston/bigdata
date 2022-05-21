package com.xubowen.consume;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.Arrays;
import java.util.Properties;

/**
 * @author XuBowen
 * @date 2021/11/20 19:17
 */
public class HighConsumer {
    public static void main(String[] args) {
        // 创建配置对象
        Properties prop = new Properties();
        prop.setProperty("bootstrap.servers","CentOS-02:9092");
        prop.setProperty("key.deserializer","org.apache.kafka.common.serialization.StringDeserializer");
        prop.setProperty("value.deserializer","org.apache.kafka.common.serialization.StringDeserializer");
        prop.setProperty("group.id","xu");
        prop.put("enable.auto.commit", "true");
        prop.put("auto.commit.interval.ms", "1000");


        // 创建消费者对象
        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(prop);

        // 订阅主题
        consumer.subscribe(Arrays.asList("xubowen"));

        while (true){
            ConsumerRecords<String, String> records = consumer.poll(500);
            for (ConsumerRecord<String, String> record : records) {
                System.out.println(record);
            }
        }
    }

}
