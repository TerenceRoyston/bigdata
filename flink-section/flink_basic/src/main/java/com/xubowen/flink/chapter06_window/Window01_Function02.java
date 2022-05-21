package com.xubowen.flink.chapter06_window;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.util.HashSet;

/**
 * @author XuBowen
 * @date 2022/4/24 23:51
 */
public class Window01_Function02 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 设置并行度
        env.setParallelism(1);
        Order o1 = new Order(1001L, "aa", 12.5);
        Order o2 = new Order(1002L, "bb", 23.5);
        Order o3 = new Order(1003L, "cc", 33.5);
        DataStreamSource<Order> source = env.fromElements(o1, o2, o3);
        SingleOutputStreamOperator<Order> stream = source.assignTimestampsAndWatermarks(WatermarkStrategy.<Order>forBoundedOutOfOrderness(Duration.ofSeconds(2))
                .withTimestampAssigner(new SerializableTimestampAssigner<Order>() {
                    @Override
                    public long extractTimestamp(Order order, long l) {
                        return order.getTimeStamp();
                    }
                }));

        // stream.keyBy(Order::getName)
        stream.keyBy(data -> true)
                .window(TumblingEventTimeWindows.of(Time.seconds(2)))
                .process(new MyProcessWindowFunction())
                .print();

        env.execute();
    }

    // 自定义全窗口函数
    public static class MyProcessWindowFunction extends ProcessWindowFunction<Order,String,Boolean, TimeWindow>{
        @Override
        public void process(Boolean aBoolean, Context context, Iterable<Order> elements, Collector<String> collector) throws Exception {
            HashSet<String> userSet = new HashSet<>();
            for (Order order : elements) {
                userSet.add(order.name);
            }

            int uv = userSet.size();
            long start = context.window().getStart();
            long end = context.window().getEnd();
            collector.collect(start+"--"+end+"--"+uv);
        }
    }

}
