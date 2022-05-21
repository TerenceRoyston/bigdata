package com.xubowen.flink.chapter05_api.source;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author XuBowen
 * @date 2022/4/12 22:59
 */
public class Api01_Source {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //
        DataStreamSource<Integer> source = env.fromElements(1, 2, 3, 4, 5);
        source.print();

        System.out.println(System.getProperty("user.dir"));
        SingleOutputStreamOperator<Integer> map = source.map(x -> {
            return x * 2;
        });
        env.execute("source");
    }
}
