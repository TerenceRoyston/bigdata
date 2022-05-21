package com.xubowen.flink.chapter08_stream;

import com.xubowen.flink.MySource;
import com.xubowen.flink.WaterSensor;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
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
public class Stream01_Split01 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        SingleOutputStreamOperator<WaterSensor> stream = env.addSource(new MySource())
                .assignTimestampsAndWatermarks(WatermarkStrategy.<WaterSensor>forBoundedOutOfOrderness(Duration.ZERO)
                        .withTimestampAssigner(new SerializableTimestampAssigner<WaterSensor>() {
                            @Override
                            public long extractTimestamp(WaterSensor waterSensor, long l) {
                                return waterSensor.getTs();
                            }
                        }));

        // 定义侧输出流标签 注意要以匿名内部类方式实现
         OutputTag<WaterSensor> other = new OutputTag<WaterSensor>("other"){};


        SingleOutputStreamOperator<WaterSensor> processedStream = stream.process(new ProcessFunction<WaterSensor, WaterSensor>() {
            @Override
            public void processElement(WaterSensor waterSensor, Context context, Collector<WaterSensor> collector) throws Exception {
                if (waterSensor.getId().equals("5")) {
                    context.output(other, waterSensor);
                } else {
                    collector.collect(waterSensor);
                }
            }
        });

        processedStream.print("main");
        processedStream.getSideOutput(other).print("other");

        env.execute();
    }
}
