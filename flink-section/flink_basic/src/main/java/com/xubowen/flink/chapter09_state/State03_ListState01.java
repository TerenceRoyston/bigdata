package com.xubowen.flink.chapter09_state;

import com.xubowen.flink.MySource;
import com.xubowen.flink.WaterSensor;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
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
public class State03_ListState01 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // create two streams
        SingleOutputStreamOperator<Tuple3<String, String, Long>> stream1 = env.fromElements(
                Tuple3.of("a", "stream-1", 1000L),
                Tuple3.of("b", "stream-1", 2000L)
        ).assignTimestampsAndWatermarks(WatermarkStrategy.<Tuple3<String, String, Long>>forMonotonousTimestamps()
                .withTimestampAssigner(new SerializableTimestampAssigner<Tuple3<String, String, Long>>() {
                    @Override
                    public long extractTimestamp(Tuple3<String, String, Long> element, long l) {
                        return element.f2;
                    }
                }));

        SingleOutputStreamOperator<Tuple3<String, String, Long>> stream2 = env.fromElements(
                Tuple3.of("a", "stream-2", 3000L),
                Tuple3.of("b", "stream-2", 4000L)
        ).assignTimestampsAndWatermarks(WatermarkStrategy.<Tuple3<String, String, Long>>forMonotonousTimestamps()
                .withTimestampAssigner(new SerializableTimestampAssigner<Tuple3<String, String, Long>>() {
                    @Override
                    public long extractTimestamp(Tuple3<String, String, Long> element, long l) {
                        return element.f2;
                    }
                }));

        //
        stream1.keyBy(x -> x.f0)
                .connect(stream2.keyBy(x -> x.f0))
                .process(new CoProcessFunction<Tuple3<String, String, Long>, Tuple3<String, String, Long>, String>() {

                    // define two listState to save the elements which were already arrived
                    ListState<Tuple3<String, String, Long>> stream1List;
                    ListState<Tuple3<String, String, Long>> stream2List;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        stream1List=getRuntimeContext().getListState(new ListStateDescriptor<Tuple3<String, String, Long>>("stream-1", Types.TUPLE(Types.STRING,Types.STRING,Types.LONG)));
                        stream2List=getRuntimeContext().getListState(new ListStateDescriptor<Tuple3<String, String, Long>>("stream-2", Types.TUPLE(Types.STRING,Types.STRING,Types.LONG)));

                    }

                    @Override
                    public void processElement1(Tuple3<String, String, Long> left, Context context, Collector<String> collector) throws Exception {
                        for (Tuple3<String, String, Long> right : stream2List.get()) {
                            collector.collect(left+" => "+right);
                        }

                        stream1List.add(left);
                    }

                    @Override
                    public void processElement2(Tuple3<String, String, Long> right, Context context, Collector<String> collector) throws Exception {
                        for (Tuple3<String, String, Long> left : stream1List.get()) {
                            collector.collect(right +" => "+left);
                        }
                        stream2List.add(right);
                    }
                }).print();


        env.execute();


    }

}
