package com.xubowen.flink.chapter05_api.transform;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * @author XuBowen
 * @date 2022/4/16 12:57
 */
public class Api03_FlatMap01 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 设置并行度
        env.setParallelism(1);
        DataStreamSource<Integer> source = env.fromElements(1, 2, 3, 4, 5);

        // 使用lambda表达式
        SingleOutputStreamOperator<String> result = source.flatMap((Integer num, Collector<String> out) -> {
            out.collect(num + "||");
        }).returns(Types.STRING);


        result.print();
        env.execute();
    }


}
