package com.xubowen.produce;

import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;

import java.util.Map;

/**
 * @author XuBowen
 * @date 2021/11/20 16:40
 */
public class MyPartitioner implements Partitioner {
    public int partition(String s, Object o, byte[] bytes, Object o1, byte[] bytes1, Cluster cluster) {
        // 自定义分区算法
        return 2;
    }

    public void close() {

    }

    public void configure(Map<String, ?> map) {

    }
}
