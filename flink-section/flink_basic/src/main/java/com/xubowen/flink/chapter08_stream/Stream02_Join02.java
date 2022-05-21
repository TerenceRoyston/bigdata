package com.xubowen.flink.chapter08_stream;

import com.xubowen.flink.MySource;
import com.xubowen.flink.WaterSensor;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.co.CoMapFunction;
import org.apache.flink.util.Collector;

import java.time.Duration;

/**
 * @author XuBowen
 * @date 2022/5/2 20:08
 */
public class Stream02_Join02 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<Integer> stream1 = env.fromElements(1, 2, 3, 4, 5);
        DataStreamSource<String> stream2 = env.fromElements("a", "b", "c", "d", "e");

        stream1.connect(stream2).map(new CoMapFunction<Integer, String, String>() {
            @Override
            public String map1(Integer v1) throws Exception {
                return v1.toString()+" => ";
            }

            @Override
            public String map2(String v2) throws Exception {
                return v2+" => ";
            }
        }).print();


        env.execute();
    }
}
