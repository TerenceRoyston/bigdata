package com.xubowen.flink.chapter07_processfunction;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

import java.time.Duration;

/**
 * @author XuBowen
 * @date 2022/5/1 15:11
 */
public class Process01_Function01 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
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
        stream.process(new MyProcessFunction())
                .print();

        env.execute();

    }

    public static class MyProcessFunction extends ProcessFunction<Order ,String>{
        @Override
        public void processElement(Order order, Context context, Collector<String> collector) throws Exception {
            collector.collect("name => " + order.name);
            System.out.println("timestamp => " + context.timestamp());
            System.out.println("watermark => " + context.timerService().currentWatermark());
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


