package com.xubowen.flink.chapter05_api.transform;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author XuBowen
 * @date 2022/4/16 12:57
 */
public class Api05_Reduce01 {
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
        // 注意是一个累加的过程
        SingleOutputStreamOperator<Tuple2<String, Integer>> result = source.keyBy(num -> num.f0).reduce((value1, value2) -> Tuple2.of(value1.f0, (value1.f1 + value2.f1)));

        result.print();
        env.execute();
    }


}
