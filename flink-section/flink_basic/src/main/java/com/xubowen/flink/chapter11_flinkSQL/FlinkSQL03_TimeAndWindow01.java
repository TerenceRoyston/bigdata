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

import static org.apache.flink.table.api.Expressions.$;

/**
 * @author XuBowen
 * @date 2022/5/15 14:39
 */
public class FlinkSQL03_TimeAndWindow01 {
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

        stream.print("main =");

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        //Table eventTable = tableEnv.fromDataStream(stream, $("id"), $("ts"), $("vc"), $("et").rowtime());


        String creatTable = "create table source(id INT,ts INT,vc INT , et as TO_TIMESTAMP(FROM_UNIXTIME(ts/1000)), watermark for et as et-interval '1' second ) with('connector'='filesystem','path'='F:\\ProgrammingSoftware\\IntelliJ IDEA Workspace\\bigdata\\flink-section\\flink_basic\\data\\word','format'='csv')";
        tableEnv.executeSql(creatTable);
        Table aggTable = tableEnv.sqlQuery("select * from source");
        tableEnv.toChangelogStream(aggTable).print("agg =");

        env.execute();
    }
}
