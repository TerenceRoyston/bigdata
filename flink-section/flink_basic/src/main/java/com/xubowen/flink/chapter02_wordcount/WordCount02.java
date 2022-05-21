package com.xubowen.flink.chapter02_wordcount;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * @author XuBowen
 * @date 2022/4/8 21:23
 */
public class WordCount02 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> fileStreamDataSource = env.readTextFile("F:\\ProgrammingSoftware\\IntelliJ IDEA Workspace\\bigdata\\flink-section\\flink_basic\\data\\word");
        SingleOutputStreamOperator<Tuple2<String, Integer>> flatDatas = fileStreamDataSource.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public void flatMap(String line, Collector<Tuple2<String, Integer>> out) throws Exception {
                String[] words = line.split(" ");
                for (String word : words) {
                    out.collect(new Tuple2<String, Integer>(word, 1));
                }
            }
        });

        KeyedStream<Tuple2<String, Integer>, Tuple> ksData = flatDatas.keyBy(0);
        SingleOutputStreamOperator<Tuple2<String, Integer>> sum = ksData.sum(1);
        sum.print();

        env.execute();

    }
}
