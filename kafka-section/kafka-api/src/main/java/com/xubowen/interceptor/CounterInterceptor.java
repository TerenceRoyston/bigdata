package com.xubowen.interceptor;

import org.apache.kafka.clients.producer.ProducerInterceptor;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Map;

/**
 * @author XuBowen
 * @date 2021/11/23 22:44
 */
public class CounterInterceptor implements ProducerInterceptor<String,String> {

    private int errorCounter = 0;
    private int successCounter = 0;


    public ProducerRecord<String, String> onSend(ProducerRecord<String, String> record) {
        return record;
    }

    public void onAcknowledgement(RecordMetadata recordMetadata, Exception e) {
        // 统计成功和失败的次数
        if (e == null) {
            successCounter++;
        } else {
            errorCounter++;
        }

    }

    public void close() {
        System.out.println("Successful sent: " + successCounter);
        System.out.println("Failed sent: " + errorCounter);
    }

    public void configure(Map<String, ?> map) {

    }
}
