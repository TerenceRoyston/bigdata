package com.xubowen.flink.chapter09_state;

import com.xubowen.flink.MySource;
import com.xubowen.flink.WaterSensor;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.time.Duration;

/**
 * @author XuBowen
 * @date 2022/5/4 16:08
 */
public class State02_ValueState01 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<WaterSensor> source = env.addSource(new MySource());
        SingleOutputStreamOperator<WaterSensor> stream = source.assignTimestampsAndWatermarks(WatermarkStrategy.<WaterSensor>forBoundedOutOfOrderness(Duration.ZERO)
                .withTimestampAssigner(new SerializableTimestampAssigner<WaterSensor>() {
                    @Override
                    public long extractTimestamp(WaterSensor waterSensor, long l) {
                        return waterSensor.getTs();
                    }
                }));

        stream.keyBy(WaterSensor::getId)
                .process(new PeriodicResult())
                .print();

        env.execute();


    }

    private static class PeriodicResult extends KeyedProcessFunction<String, WaterSensor, String> {
        ValueState<Long> countState;
        ValueState<Long> timerState;

        @Override
        public void open(Configuration parameters) throws Exception {
            countState = getRuntimeContext().getState(new ValueStateDescriptor<Long>("count", Long.class));
            timerState = getRuntimeContext().getState(new ValueStateDescriptor<Long>("timer", Long.class));
        }

        @Override
        public void processElement(WaterSensor waterSensor, Context context, Collector<String> collector) throws Exception {
            countState.update(countState.value() == null ? 1 : countState.value() + 1);
            // register a timer
            if (timerState.value() == null) {
                context.timerService().registerEventTimeTimer(waterSensor.getTs() + 10 * 1000L);
                timerState.update(waterSensor.getTs() + 10 * 1000L);
            }
            collector.collect(waterSensor.toString());
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
            System.out.println("=====timer is triggered=====");
            out.collect(ctx.getCurrentKey() + " => " + countState.value());
            timerState.clear();
            System.out.println("============================");
        }
    }
}
