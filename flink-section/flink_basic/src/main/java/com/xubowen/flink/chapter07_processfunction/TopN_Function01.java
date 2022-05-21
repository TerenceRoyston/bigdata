package com.xubowen.flink.chapter07_processfunction;

import com.xubowen.flink.MySource;
import com.xubowen.flink.WaterSensor;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.api.windowing.windows.Window;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;

/**
 * @author XuBowen
 * @date 2022/5/1 20:28
 */
public class TopN_Function01 {
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
        stream.map(x -> x.getId())
                .windowAll(SlidingEventTimeWindows.of(Time.seconds(10),Time.seconds(5)))
                .aggregate(new UrlHashMapCountAgg(),new UrlAllWindowResult())
                .print();

        env.execute();
    }

    private static class UrlHashMapCountAgg implements AggregateFunction<String, HashMap<String,Long>, ArrayList<Tuple2<String,Long>>> {

        @Override
        public HashMap<String, Long> createAccumulator() {
            return new HashMap<String, Long>();
        }

        @Override
        public HashMap<String, Long> add(String s, HashMap<String, Long> stringLongHashMap) {
            stringLongHashMap.put(s,stringLongHashMap.getOrDefault(s,0L)+1);
            return stringLongHashMap;
        }

        @Override
        public ArrayList<Tuple2<String, Long>> getResult(HashMap<String, Long> stringLongHashMap) {
            ArrayList<Tuple2<String, Long>> result = new ArrayList<>();
            for (String key : stringLongHashMap.keySet()) {
                result.add(Tuple2.of(key,stringLongHashMap.get(key)));
            }
            result.sort(new Comparator<Tuple2<String, Long>>() {
                @Override
                public int compare(Tuple2<String, Long> o1, Tuple2<String, Long> o2) {
                    return o2.f1.intValue()-o1.f1.intValue();
                }
            });
            return result;
        }

        @Override
        public HashMap<String, Long> merge(HashMap<String, Long> stringLongHashMap, HashMap<String, Long> acc1) {
            return null;
        }
    }

    private static class UrlAllWindowResult extends ProcessAllWindowFunction<ArrayList<Tuple2<String, Long>>,String, TimeWindow> {

        @Override
        public void process(Context context, Iterable<ArrayList<Tuple2<String, Long>>> iterable, Collector<String> collector) throws Exception {
            ArrayList<Tuple2<String, Long>> list = iterable.iterator().next();
            collector.collect(list.get(0).f0+" => "+list.get(1).f1);
        }
    }
}
