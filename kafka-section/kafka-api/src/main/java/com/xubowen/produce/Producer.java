package com.xubowen.produce;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Properties;

/**
 * @author XuBowen
 * @date 2021/11/8 22:50
 */
public class Producer {
    public static void main(String[] args) {

        // 创建配置对象
        Properties prop = new Properties();
        prop.setProperty("bootstrap.servers","CentOS-02:9092");
        prop.setProperty("key.serializer","org.apache.kafka.common.serialization.StringSerializer");
        prop.setProperty("value.serializer","org.apache.kafka.common.serialization.StringSerializer");
        // 应答机制
        prop.setProperty("acks","1");

        // 自定义分区
        // prop.setProperty("partitioner.class","com.xubowen.produce.MyPartitioner");

        // 创建生产者
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(prop);

        // 准备数据
        // 不指定分区
        ProducerRecord<String, String> record = new ProducerRecord<String, String>("xubowen","hello scala");
        // 指定分区
        // ProducerRecord<String, String> record = new ProducerRecord<String, String>("xubowen",1,null,"hello scala");
        // 自定义分区
        // ProducerRecord<String, String> record = new ProducerRecord<String, String>("xubowen","hello scala");

        // 生产数据
        // 异步
        producer.send(record);

        // 异步 增加回调函数
        producer.send(record,new MyCallback());



        // 关闭生产者
        producer.close();



    }
}

/**
 * Runnable接口run方法无返回值
 * Callable接口方法可以获取返回值
 */
class MyCallback implements Callback{
    // 回调方法
    public void onCompletion(RecordMetadata recordMetadata, Exception e) {
        // 发送到哪一个分区
        System.out.println(recordMetadata.partition());
        // 数据偏移量
        System.out.println(recordMetadata.offset());
    }
}
