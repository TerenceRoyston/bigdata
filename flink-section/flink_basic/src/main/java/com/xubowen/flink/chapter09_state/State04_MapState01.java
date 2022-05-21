package com.xubowen.flink.chapter09_state;

import com.xubowen.flink.MySource;
import com.xubowen.flink.WaterSensor;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import org.apache.flink.util.Collector;

import java.time.Duration;

/**
 * @author XuBowen
 * @date 2022/5/4 16:08
 */
public class State04_MapState01 {
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

        stream.keyBy(WaterSensor::getId)
                .process(new FakeWindow(5000L))
                .print();


        env.execute();
    }


    private static class FakeWindow extends KeyedProcessFunction<String, WaterSensor, String> {

        Long windowSize;

        MapState<Long, Long> windowMap;

        public FakeWindow(long l) {
            this.windowSize = l;
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            windowMap = getRuntimeContext().getMapState(new MapStateDescriptor<Long, Long>("window_map", Long.class, Long.class));
        }

        @Override
        public void processElement(WaterSensor waterSensor, Context context, Collector<String> collector) throws Exception {
            System.out.println(waterSensor);

            // get start point and end point of window
            Long windowStart = waterSensor.getTs() / windowSize * windowSize;
            Long windowEnd = windowStart + windowSize;

            // end point need to minus 1
            context.timerService().registerEventTimeTimer(windowEnd - 1);

            if (windowMap.contains(windowStart)) {
                windowMap.put(windowStart, windowMap.get(windowStart) + 1);
            } else {
                windowMap.put(windowStart, 1L);
            }
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
            Long windowEnd = timestamp + 1;
            Long windowStart = windowEnd - windowSize;
            Long count = windowMap.get(windowStart);
            out.collect("窗口" + windowStart + "~" + windowEnd + " id => " + ctx.getCurrentKey() + " count => " + count);

            // remove window data to simulate closing window
            windowMap.remove(windowStart);
        }
    }
}
