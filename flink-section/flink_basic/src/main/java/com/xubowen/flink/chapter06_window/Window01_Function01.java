package com.xubowen.flink.chapter06_window;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.time.Duration;
import java.util.HashSet;

/**
 * @author XuBowen
 * @date 2022/4/24 23:51
 */
public class Window01_Function01 {
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
                .window(TumblingEventTimeWindows.of(Time.seconds(1)))
                .aggregate(new MyAggr())
                .print();

        env.execute();


    }

    // 自定义聚合函数 计算pv/uv
    public static class MyAggr implements AggregateFunction<Order, Tuple2<Long, HashSet<String>>, Double> {

        @Override
        public Tuple2<Long, HashSet<String>> createAccumulator() {
            return Tuple2.of(0L, new HashSet<>());
        }

        @Override
        public Tuple2<Long, HashSet<String>> add(Order order, Tuple2<Long, HashSet<String>> accumulator) {
            accumulator.f1.add(order.name);
            return Tuple2.of(accumulator.f0 + 1, accumulator.f1);
        }

        @Override
        public Double getResult(Tuple2<Long, HashSet<String>> accumulator) {
            return (double) accumulator.f0 / accumulator.f1.size();
        }

        @Override
        public Tuple2<Long, HashSet<String>> merge(Tuple2<Long, HashSet<String>> longHashSetTuple2, Tuple2<Long, HashSet<String>> acc1) {
            return null;
        }
    }
}

@Data
@AllArgsConstructor
@NoArgsConstructor
class Order {
    long timeStamp;
    String name;
    Double Value;
}
