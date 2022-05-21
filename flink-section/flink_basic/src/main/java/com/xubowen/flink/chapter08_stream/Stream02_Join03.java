package com.xubowen.flink.chapter08_stream;

import com.xubowen.flink.WaterSensor;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoMapFunction;
import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import org.apache.flink.util.Collector;

import java.time.Duration;

/**
 * @author XuBowen
 * @date 2022/5/2 20:08
 */
public class Stream02_Join03 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<Tuple3<String, String, Long>> stream1 = env.fromElements(
                Tuple3.of("order-1", "app", 1000L),
                Tuple3.of("order-2", "app", 2000L)
        );

        DataStreamSource<Tuple3<String, String, Long>> stream2 = env.fromElements(
                Tuple3.of("order-1", "pay", 3000L),
                Tuple3.of("order-3", "pay", 4000L)
        );

        stream1.connect(stream2)
                .keyBy(x -> x.f0,x -> x.f0)
                .process(new orderMatchResult())
                .print();


        env.execute();
    }

    private static class orderMatchResult extends CoProcessFunction<Tuple3<String, String, Long>,Tuple3<String, String, Long>,String> {

        ValueState<Tuple3<String, String, Long>> orderEvent;
        ValueState<Tuple3<String, String, Long>> payEvent;

        @Override
        public void open(Configuration parameters) throws Exception {
            orderEvent=getRuntimeContext().getState(
                    new ValueStateDescriptor<Tuple3<String, String, Long>>("app", Types.TUPLE(Types.STRING,Types.STRING,Types.LONG))
            );

            payEvent=getRuntimeContext().getState(
                    new ValueStateDescriptor<Tuple3<String, String, Long>>("pay", Types.TUPLE(Types.STRING,Types.STRING,Types.LONG))
            );

            System.out.println(orderEvent.toString());
        }

        @Override
        public void processElement1(Tuple3<String, String, Long> stringStringLongTuple3, Context context, Collector<String> collector) throws Exception {
            if (payEvent.value()!=null){
                collector.collect("对账成功"+stringStringLongTuple3+" => "+payEvent.value());
                payEvent.clear();
            }
        }

        @Override
        public void processElement2(Tuple3<String, String, Long> stringStringLongTuple3, Context context, Collector<String> collector) throws Exception {
            if (orderEvent.value()!=null){
                collector.collect("对账成功"+stringStringLongTuple3+" => "+orderEvent.value());
                orderEvent.clear();
            }
        }
    }
}
