package com.xubowen.flink.chapter07_processfunction;

import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

/**
 * @author XuBowen
 * @date 2022/5/1 16:47
 */
public class Process01_Function02 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        Order o1 = new Order(1001L, "aa", 12.5);
        Order o2 = new Order(1002L, "bb", 23.5);
        Order o3 = new Order(1003L, "cc", 33.5);
        Order o4 = new Order(1004L, "aa", 43.5);
        Order o5 = new Order(1005L, "bb", 46.5);

        SingleOutputStreamOperator<Order> source = env.fromElements(o1, o2, o3,o4,o5);
        source.keyBy(x -> x.name).process(new MyKeyedFunction()).print();
        Thread.sleep(5000);
        env.execute();
    }

    public static class MyKeyedFunction extends KeyedProcessFunction<String, Order, String> {
        @Override
        public void processElement(Order order, Context context, Collector<String> collector) throws Exception {
            long currentProcessingTime = context.timerService().currentProcessingTime();
            collector.collect(context.getCurrentKey() + "数据到达时间 => " + currentProcessingTime);

            // 注册10s定时器
            context.timerService().registerProcessingTimeTimer(currentProcessingTime + 1 * 1L);
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
            out.collect(ctx.getCurrentKey() + "定时器触发时间 => " + timestamp);
        }
    }
}
