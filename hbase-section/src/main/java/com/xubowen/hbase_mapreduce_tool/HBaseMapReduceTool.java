package com.xubowen.hbase_mapreduce_tool;

import com.xubowen.hbase_mapreduce_mapper.ScanMapper;
import com.xubowen.hbase_mapreduce_reducer.InsertReducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobStatus;
import org.apache.hadoop.util.Tool;

/**
 * @author XuBowen
 * @date 2021/12/5 11:51
 */
public class HBaseMapReduceTool implements Tool {

    public int run(String[] strings) throws Exception {
        Job job = Job.getInstance();
        job.setJarByClass(HBaseMapReduceTool.class);

        // mapper
        TableMapReduceUtil.initTableMapperJob(
                "zte",
                new Scan(),
                ScanMapper.class,
                ImmutableBytesWritable.class,
                Put.class,
                job
        );

        // reducer
        TableMapReduceUtil.initTableReducerJob(
            "zte_copy",
                InsertReducer.class,
                job
        );

        // 执行作业
        boolean flag = job.waitForCompletion(true);
        return flag? JobStatus.State.SUCCEEDED.getValue():JobStatus.State.FAILED.getValue();
    }

    public void setConf(Configuration configuration) {

    }

    public Configuration getConf() {
        return null;
    }
}
