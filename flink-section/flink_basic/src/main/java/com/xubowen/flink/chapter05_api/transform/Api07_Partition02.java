package com.xubowen.flink.chapter05_api.transform;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

/**
 * @author XuBowen
 * @date 2022/4/16 12:57
 */
public class Api07_Partition02 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 设置并行度
        env.setParallelism(2);
        DataStreamSource<Integer> source = env.addSource(new MyParallelSource());
        // 广播，每个分区都会被分到
        // source.broadcast().print().setParallelism(3);

        // 全局
        source.global().print().setParallelism(4);

        env.execute();
    }

    public static class MyParallelSource extends RichParallelSourceFunction<Integer> {
        @Override
        public void run(SourceContext<Integer> sourceContext) throws Exception {
            for (int i = 0; i < 8; i++) {
                if (i % 2 == getRuntimeContext().getIndexOfThisSubtask()) {
                    sourceContext.collect(i);
                }
            }
        }

        @Override
        public void cancel() {

        }
    }


}
