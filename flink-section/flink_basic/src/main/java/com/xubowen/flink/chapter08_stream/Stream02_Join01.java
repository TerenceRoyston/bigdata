package com.xubowen.flink.chapter08_stream;

import com.xubowen.flink.MySource;
import com.xubowen.flink.WaterSensor;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.time.Duration;

/**
 * @author XuBowen
 * @date 2022/5/2 20:08
 */
public class Stream02_Join01 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        SingleOutputStreamOperator<WaterSensor> stream = env.addSource(new MySource())
                .assignTimestampsAndWatermarks(WatermarkStrategy.<WaterSensor>forBoundedOutOfOrderness(Duration.ofSeconds(2))
                        .withTimestampAssigner(new SerializableTimestampAssigner<WaterSensor>() {
                            @Override
                            public long extractTimestamp(WaterSensor waterSensor, long l) {
                                return waterSensor.getTs();
                            }
                        }));


        SingleOutputStreamOperator<WaterSensor> stream2 = env.addSource(new MySource())
                .assignTimestampsAndWatermarks(WatermarkStrategy.<WaterSensor>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                        .withTimestampAssigner(new SerializableTimestampAssigner<WaterSensor>() {
                            @Override
                            public long extractTimestamp(WaterSensor waterSensor, long l) {
                                return waterSensor.getTs();
                            }
                        }));


        stream.union(stream2).process(new ProcessFunction<WaterSensor, String>() {
            @Override
            public void processElement(WaterSensor waterSensor, Context context, Collector<String> collector) throws Exception {
                collector.collect(waterSensor+"时间戳 => "+context.timerService().currentWatermark());
            }
        }).print();




        env.execute();
    }
}
