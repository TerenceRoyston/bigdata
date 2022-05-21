package com.xubowen.hbase_mapreduce_application;

import com.xubowen.hbase_mapreduce_tool.HBaseMapReduceTool;
import org.apache.hadoop.util.ToolRunner;

/**
 * @author XuBowen
 * @date 2021/12/5 11:49
 */
public class Table2TableApplication {
    public static void main(String[] args) throws Exception {
        // ToolRunner运行MR
        ToolRunner.run(new HBaseMapReduceTool(),args);
    }
}
