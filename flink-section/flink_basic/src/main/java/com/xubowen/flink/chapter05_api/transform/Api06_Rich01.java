package com.xubowen.flink.chapter05_api.transform;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author XuBowen
 * @date 2022/4/16 12:57
 */
public class Api06_Rich01 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 设置并行度
        env.setParallelism(1);
        Tuple2<String, Integer> t1 = new Tuple2<>("a", 11);
        Tuple2<String, Integer> t11 = new Tuple2<>("a", 1);
        Tuple2<String, Integer> t2 = new Tuple2<>("b",2);
        Tuple2<String, Integer> t3 = new Tuple2<>("c",3);
        Tuple2<String, Integer> t33 = new Tuple2<>("c",33);
         DataStreamSource<Tuple2<String, Integer>> source = env.fromElements(t1, t11,t2, t3,  t33);

        // 使用lambda表达式
        SingleOutputStreamOperator<Tuple2<String, Integer>> result = source.map(new MyRichMapper());

        result.print();
        env.execute();
    }

    public static class MyRichMapper extends RichMapFunction<Tuple2<String, Integer>,Tuple2<String, Integer>>{
        @Override
        public void open(Configuration parameters) throws Exception {
            System.out.println("map富函数开启");
        }

        @Override
        public Tuple2<String, Integer> map(Tuple2<String, Integer> t) throws Exception {
            return Tuple2.of(t.f0,t.f1*100);
        }

        @Override
        public void close() throws Exception {
            System.out.println("map富函数关闭");
        }
    }


}
