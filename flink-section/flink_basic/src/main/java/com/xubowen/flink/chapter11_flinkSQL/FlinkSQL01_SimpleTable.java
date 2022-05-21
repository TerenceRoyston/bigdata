package com.xubowen.flink.chapter11_flinkSQL;

import com.xubowen.flink.MySource;
import com.xubowen.flink.WaterSensor;
import com.xubowen.flink.chapter09_state.State01_Test01;
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
 * @date 2022/5/12 22:02
 */
public class FlinkSQL01_SimpleTable {
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

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        Table eventTable = tableEnv.fromDataStream(stream);
        Table resTable = tableEnv.sqlQuery("select id,ts from " + eventTable);
        tableEnv.toDataStream(resTable).print();


        env.execute();
    }
}
