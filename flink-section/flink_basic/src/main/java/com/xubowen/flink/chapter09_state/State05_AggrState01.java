package com.xubowen.flink.chapter09_state;

import com.xubowen.flink.MySource;
import com.xubowen.flink.WaterSensor;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.*;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.time.Duration;

/**
 * @author XuBowen
 * @date 2022/5/4 16:08
 */
public class State05_AggrState01 {
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
                .flatMap(new AvgResult(5L))
                .print();


        env.execute();
    }

    public static class AvgResult extends RichFlatMapFunction<WaterSensor, String> {
        Long count;

        public AvgResult(Long count) {
            this.count = count;
        }

        AggregatingState<WaterSensor, Long> avgState;
        ValueState<Long> countState;

        @Override
        public void open(Configuration parameters) throws Exception {
            avgState = getRuntimeContext().getAggregatingState(new AggregatingStateDescriptor<WaterSensor, Tuple2<Long, Long>, Long>(
                    "aggr",
                    new AggregateFunction<WaterSensor, Tuple2<Long, Long>, Long>() {
                        @Override
                        public Tuple2<Long, Long> createAccumulator() {
                            return Tuple2.of(0L, 0L);
                        }

                        @Override
                        public Tuple2<Long, Long> add(WaterSensor waterSensor, Tuple2<Long, Long> longLongTuple2) {
                            return Tuple2.of(longLongTuple2.f0 + waterSensor.getTs(), longLongTuple2.f1 + 1);
                        }

                        @Override
                        public Long getResult(Tuple2<Long, Long> longLongTuple2) {
                            return longLongTuple2.f0 / longLongTuple2.f1;
                        }

                        @Override
                        public Tuple2<Long, Long> merge(Tuple2<Long, Long> longLongTuple2, Tuple2<Long, Long> acc1) {
                            return null;
                        }
                    },
                    Types.TUPLE(Types.LONG, Types.LONG)
            ));

            countState = getRuntimeContext().getState(new ValueStateDescriptor<Long>("value", Long.class));
        }

        @Override
        public void flatMap(WaterSensor waterSensor, Collector<String> collector) throws Exception {
            System.out.println(waterSensor);
            Long currCount = countState.value();
            currCount = (currCount == null ? 1L : currCount + 1L);
            countState.update(currCount);
            avgState.add(waterSensor);

            if (currCount.equals(count)) {
                collector.collect(waterSensor.getId() + " => " + count + " => " + avgState.get());
                countState.clear();
                avgState.clear();
            }

        }
    }


}
