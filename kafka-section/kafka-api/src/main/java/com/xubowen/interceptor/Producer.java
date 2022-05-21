package com.xubowen.interceptor;

import org.apache.kafka.clients.producer.*;

import java.util.ArrayList;
import java.util.List;
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

        // 2 构建拦截链
        List<String> interceptors = new ArrayList<String>();
        interceptors.add("com.xubowen.interceptor.CounterInterceptor");
        interceptors.add("com.xubowen.interceptor.TimeInterceptor");
        prop.put(ProducerConfig.INTERCEPTOR_CLASSES_CONFIG, interceptors);


        // 创建生产者
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(prop);

        // 准备数据
        // 不指定分区
        ProducerRecord<String, String> record = new ProducerRecord<String, String>("xubowen","hello spark");


        // 生产数据
        // 异步
        producer.send(record);

        // 关闭生产者
        producer.close();



    }
}


