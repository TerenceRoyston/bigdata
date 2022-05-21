package com.xubowen.hbase_mapreduce_reducer;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.io.NullWritable;

import java.io.IOException;

/**
 * @author XuBowen
 * @date 2021/12/5 12:09
 */
public class InsertReducer extends TableReducer<ImmutableBytesWritable, Put, NullWritable> {
    @Override
    protected void reduce(ImmutableBytesWritable key, Iterable<Put> values, Context context) throws IOException, InterruptedException {
        // 运行reducer，新增数据
        for (Put put : values) {
            context.write(NullWritable.get(),put);
        }
    }
}
