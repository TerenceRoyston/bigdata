package com.xubowen.flink.chapter09_state;

import com.xubowen.flink.MySource;
import com.xubowen.flink.WaterSensor;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.*;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.time.Duration;
import java.util.Iterator;

/**
 * @author XuBowen
 * @date 2022/5/3 19:34
 */
public class State01_Test01 {
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
                .map(new MyMapFunction())
                .print();

        env.execute();


    }

    private static class MyMapFunction extends RichMapFunction<WaterSensor, Tuple2<String, Long>> {

        // 定义状态
        ValueState<WaterSensor> myValueState;
        ListState<WaterSensor> myListState;
        MapState<String, Long> myMapState;
        ReducingState<WaterSensor> myReduceState;
        AggregatingState<WaterSensor, String> myAggregatingState;

        @Override
        public void open(Configuration parameters) throws Exception {
            myValueState = getRuntimeContext().getState(new ValueStateDescriptor<WaterSensor>("my-state", WaterSensor.class));
            myListState = getRuntimeContext().getListState(new ListStateDescriptor<WaterSensor>("my-list", WaterSensor.class));
            myMapState = getRuntimeContext().getMapState(new MapStateDescriptor<String, Long>("my-map", String.class, Long.class));
            myReduceState = getRuntimeContext().getReducingState(new ReducingStateDescriptor<WaterSensor>("my-reduce", new ReduceFunction<WaterSensor>() {
                @Override
                public WaterSensor reduce(WaterSensor value1, WaterSensor value2) throws Exception {
                    return new WaterSensor(value1.getId(), value1.getTs(), value2.getVc());

                }
            }, WaterSensor.class));

            myAggregatingState = getRuntimeContext().getAggregatingState(new AggregatingStateDescriptor<WaterSensor, Long, String>("my-aggr", new AggregateFunction<WaterSensor, Long, String>() {
                @Override
                public Long createAccumulator() {
                    return 0L;
                }

                @Override
                public Long add(WaterSensor waterSensor, Long aLong) {
                    return aLong + 1;
                }

                @Override
                public String getResult(Long aLong) {
                    return "count => " + aLong;
                }

                @Override
                public Long merge(Long aLong, Long acc1) {
                    return null;
                }
            }, Long.class));

        }

        @Override
        public Tuple2<String, Long> map(WaterSensor waterSensor) throws Exception {
            // test ValueState
            /*
            System.out.println("before"+myValueState.value());
            myValueState.update(waterSensor);
            System.out.println("after"+myValueState.value());
            */

            // test ListState
            /*
            System.out.println("=====ListState Before=====");
            Iterator<WaterSensor> iterator = myListState.get().iterator();
            while (iterator.hasNext()){
                System.out.println(iterator.next().toString());
            }
            myListState.add(waterSensor);
            System.out.println("=====ListState After=====");
            iterator = myListState.get().iterator();
            while (iterator.hasNext()){
                System.out.println(iterator.next().toString());
            }
            System.out.println("==========================");
            */

            // test MapState
            /*
            System.out.println("=====MapState Before=====");
            System.out.println(myMapState.get(waterSensor.getId()));
            myMapState.put(waterSensor.getId(), myMapState.get(waterSensor.getId()) == null ? 1 : myMapState.get(waterSensor.getId()) + 1L);
            System.out.println("=====MapState After=====");
            System.out.println(myMapState.get(waterSensor.getId()));
            System.out.println("========================");
            */

            // test AggregateState
            /*
            System.out.println("=====AggregatingState Before=====");
            System.out.println(myAggregatingState.get());
            myAggregatingState.add(waterSensor);
            System.out.println("=====AggregatingState After=====");
            System.out.println(myAggregatingState.get());
            */

            // test ReduceState
            System.out.println("=====ReduceState Before=====");
            System.out.println(myReduceState.get());
            myReduceState.add(waterSensor);
            System.out.println("=====ReduceState After=====");
            System.out.println(myReduceState.get());


            return Tuple2.of(waterSensor.getId(), waterSensor.getTs());
        }
    }
}
