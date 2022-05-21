package com.xubowen.hbase_mapreduce_mapper;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;

import java.io.IOException;

/**
 * @author XuBowen
 * @date 2021/12/5 12:06
 */
public class ScanMapper extends TableMapper<ImmutableBytesWritable, Put> {
    @Override
    protected void map(ImmutableBytesWritable key, Result result, Context context) throws IOException, InterruptedException {
        // 运行mapper，查询数据

        // 想办法把result => put
        Put put = new Put(key.get());
        for (Cell cell : result.rawCells()) {
            put.addColumn(
                    CellUtil.cloneFamily(cell),
                    CellUtil.cloneQualifier(cell),
                    CellUtil.cloneValue(cell)
            );
        }
        context.write(key,put);
    }
}
