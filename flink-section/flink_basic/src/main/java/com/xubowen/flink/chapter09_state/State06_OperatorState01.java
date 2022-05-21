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
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

/**
 * @author XuBowen
 * @date 2022/5/4 16:08
 */
public class State06_OperatorState01 {
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

        stream.print("input => ");

        stream.addSink(new MySink(10, new ArrayList<WaterSensor>()));


        env.execute();
    }


    private static class MySink implements SinkFunction<WaterSensor>, CheckpointedFunction {

        int threshold;
        List<WaterSensor> bufferElement;


        public MySink(int threshold, List<WaterSensor> bufferElement) {
            this.threshold = threshold;
            this.bufferElement = bufferElement;
        }

        ListState<WaterSensor> checkpointState;

        @Override
        public void initializeState(FunctionInitializationContext functionInitializationContext) throws Exception {
            ListStateDescriptor<WaterSensor> descriptor = new ListStateDescriptor<>("buffer_element", WaterSensor.class);
            checkpointState = functionInitializationContext.getOperatorStateStore().getListState(descriptor);


            if (functionInitializationContext.isRestored()) {
                for (WaterSensor s : checkpointState.get()) {
                    bufferElement.add(s);
                }
            }
        }

        @Override
        public void snapshotState(FunctionSnapshotContext functionSnapshotContext) throws Exception {
            checkpointState.clear();
            for (WaterSensor s : bufferElement) {
                checkpointState.add(s);
            }
        }


        @Override
        public void invoke(WaterSensor value) throws Exception {

        }

        @Override
        public void invoke(WaterSensor value, Context context) throws Exception {
            bufferElement.add(value);
            if (bufferElement.size() == threshold) {
                for (WaterSensor s : bufferElement) {
                    System.out.println(s);
                }
                bufferElement.clear();
            }
        }
    }
}
