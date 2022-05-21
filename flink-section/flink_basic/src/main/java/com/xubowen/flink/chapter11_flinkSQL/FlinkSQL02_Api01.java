package com.xubowen.flink.chapter11_flinkSQL;

import com.xubowen.flink.MySource;
import com.xubowen.flink.WaterSensor;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.time.Duration;

/**
 * @author XuBowen
 * @date 2022/5/12 22:27
 */
public class FlinkSQL02_Api01 {
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

        stream.print("main -");

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        Table eventTable = tableEnv.fromDataStream(stream);

        tableEnv.createTemporaryView("source", eventTable);
        Table aggResult = tableEnv.sqlQuery("select id,count(*) ct from source group by id");
        tableEnv.toChangelogStream(aggResult).print("count -");

        // tableEnv.executeSql("create table source(id INT,ts INT,vc INT) with('connector'='filesystem','path'='input/word','format'='csv')");
        // tableEnv.executeSql("create table out(id INT,ts INT,vc INT) with('connector'='filesystem','path'='output','format'='csv')");

        env.execute();


    }
}
