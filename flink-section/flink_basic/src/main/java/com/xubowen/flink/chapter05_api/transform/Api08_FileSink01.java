package com.xubowen.flink.chapter05_api.transform;

import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;

/**
 * @author XuBowen
 * @date 2022/4/16 12:57
 */
public class Api08_FileSink01 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 设置并行度
        env.setParallelism(1);
        DataStreamSource<Integer> source = env.fromElements(1, 2, 3, 4, 5);

        StreamingFileSink<String> fileSink = StreamingFileSink.<String>forRowFormat(new Path("F:\\ProgrammingSoftware\\IntelliJ IDEA Workspace\\bigdata\\flink-section\\flink_basic\\output"), new SimpleStringEncoder<>("UTF-8")).build();

        source.map(num -> num.toString()).addSink(fileSink);
        // result.print();
        env.execute();
    }




}
