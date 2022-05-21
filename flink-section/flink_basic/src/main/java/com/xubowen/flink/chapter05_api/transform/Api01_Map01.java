package com.xubowen.flink.chapter05_api.transform;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author XuBowen
 * @date 2022/4/16 12:57
 */
public class Api01_Map01 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 设置并行度
        env.setParallelism(1);
        DataStreamSource<Integer> source = env.fromElements(1, 2, 3, 4, 5);
        // 使用静态内部类
        // SingleOutputStreamOperator<Integer> result = source.map(new MyMapFunction());

        // 使用匿名内部类
        /*SingleOutputStreamOperator<Integer> result = source.map(new MapFunction<Integer, Integer>() {
            @Override
            public Integer map(Integer value) throws Exception {
                return value * 2;
            }
        });*/

        // 使用lambda表达式
        SingleOutputStreamOperator<Integer> result = source.map(num -> num * 2);


        result.print();
        env.execute();
    }

    public static class MyMapFunction implements MapFunction<Integer,Integer>{
        @Override
        public Integer map(Integer value) throws Exception {
            return value*2;
        }
    }
}
