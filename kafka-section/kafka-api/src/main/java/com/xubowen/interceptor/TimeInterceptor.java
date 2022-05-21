package com.xubowen.interceptor;

import org.apache.kafka.clients.producer.ProducerInterceptor;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Map;

/**
 * @author XuBowen
 * @date 2021/11/23 22:27
 */
public class TimeInterceptor implements ProducerInterceptor<String,String> {

    public ProducerRecord<String, String> onSend(ProducerRecord<String, String> record) {
        String oldValue = record.value();
        String newValue = System.currentTimeMillis() + "_" + oldValue;
        ProducerRecord<String, String> newRecord = new ProducerRecord<String, String>(record.topic(), newValue);
        return newRecord;

    }

    public void onAcknowledgement(RecordMetadata recordMetadata, Exception e) {

    }

    public void close() {

    }

    public void configure(Map<String, ?> map) {

    }
}
