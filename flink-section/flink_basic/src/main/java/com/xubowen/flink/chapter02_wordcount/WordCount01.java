package com.xubowen.flink.chapter02_wordcount;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.AggregateOperator;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.FlatMapOperator;
import org.apache.flink.api.java.operators.UnsortedGrouping;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

/**
 * @author XuBowen
 * @date 2022/4/8 21:23
 */
public class WordCount01 {
    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        DataSource<String> fileDataSource = env.readTextFile("F:\\ProgrammingSoftware\\IntelliJ IDEA Workspace\\bigdata\\flink-section\\flink_basic\\data\\word");
        FlatMapOperator<String, Tuple2<String, Integer>> wordDatas = fileDataSource.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public void flatMap(String line, Collector<Tuple2<String, Integer>> out) throws Exception {
                String[] words = line.split(" ");
                for (String word : words) {
                    out.collect(new Tuple2<String, Integer>(word, 1));
                }
            }
        });
        UnsortedGrouping<Tuple2<String, Integer>> groupDatas = wordDatas.groupBy(0);
        AggregateOperator<Tuple2<String, Integer>> result = groupDatas.sum(1);

    }
}
