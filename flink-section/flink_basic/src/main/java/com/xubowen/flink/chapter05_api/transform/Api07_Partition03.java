package com.xubowen.flink.chapter05_api.transform;

import org.apache.flink.api.common.functions.Partitioner;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author XuBowen
 * @date 2022/4/16 12:57
 */
public class Api07_Partition03 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 设置并行度
        env.setParallelism(1);
        DataStreamSource<Integer> source = env.fromElements(1, 2, 3, 4, 5);

        source.partitionCustom(new MyPartitioner(), new MySelector()).print().setParallelism(4);


        // result.print();
        env.execute();
    }

    public static class MyPartitioner implements Partitioner<Integer> {
        @Override
        public int partition(Integer key, int numPartitions) {
            return key % 2;
        }
    }

    public static class MySelector implements KeySelector<Integer, Integer> {
        @Override
        public Integer getKey(Integer value) throws Exception {
            return value;
        }
    }


}
