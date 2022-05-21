package com.xubowen.flink.chapter09_state;

import com.xubowen.flink.MySource;
import com.xubowen.flink.WaterSensor;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.*;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.optimizer.operators.MapDescriptor;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

/**
 * @author XuBowen
 * @date 2022/5/4 16:08
 */
public class State07_BroadcastState01 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        Action a1 = new Action("alice", "login");
        Action a2 = new Action("alice", "pay");
        Action a3 = new Action("bob", "login");
        Action a4 = new Action("bob", "buy");

        Pattern p1 = new Pattern("login", "pay");
        Pattern p2 = new Pattern("login", "buy");

        DataStreamSource<Action> action = env.fromElements(a1, a2, a3, a4);
        DataStreamSource<Pattern> pattern = env.fromElements(p1, p2);

        MapStateDescriptor<Void, Pattern> descriptor = new MapStateDescriptor<>("pattern", Types.VOID, Types.POJO(Pattern.class));
        // broad stream
        BroadcastStream<Pattern> broadcast = pattern.broadcast(descriptor);

        action.keyBy(x -> x.userId)
                .connect(broadcast)
                .process(new PatternDetector()).print();

        env.execute();
    }


    private static class PatternDetector extends KeyedBroadcastProcessFunction<String,Action,Pattern, Tuple2<String,Pattern>> {

        ValueState<String> myValueState;

        @Override
        public void open(Configuration parameters) throws Exception {
            myValueState=getRuntimeContext().getState(new ValueStateDescriptor<String>("last",Types.STRING));
        }

        @Override
        public void processElement(Action action, ReadOnlyContext readOnlyContext, Collector<Tuple2<String, Pattern>> collector) throws Exception {
            ReadOnlyBroadcastState<Void, Pattern> patternState = readOnlyContext.getBroadcastState(new MapStateDescriptor<>("pattern", Types.VOID, Types.POJO(Pattern.class)));
            Pattern pattern = patternState.get(null);

            // TODO
            String preAction = myValueState.value();
            if (preAction!=null && pattern!=null){
                if (pattern.action1.equals(preAction) && pattern.action2.equals(action.action)){
                    collector.collect(new Tuple2<>(readOnlyContext.getCurrentKey(),pattern));
                }
            }

            myValueState.update(action.action);
        }

        @Override
        public void processBroadcastElement(Pattern pattern, Context context, Collector<Tuple2<String, Pattern>> collector) throws Exception {
            BroadcastState<Void, Pattern> patternState = context.getBroadcastState(new MapStateDescriptor<>("pattern", Types.VOID, Types.POJO(Pattern.class)));
            patternState.put(null,pattern);
        }
    }
}

@Data
@AllArgsConstructor
@NoArgsConstructor
class Action{
    String userId;
    String action;
}

@Data
@AllArgsConstructor
@NoArgsConstructor
class Pattern{
    String action1;
    String action2;
}
